# Databricks notebook source

from databricks.sdk.service import jobs


def check_workspace_path(w, path: str) -> bool:
    """
    Returns True if a workspace object exists at the given path.
    Intended for validating WORKSPACE notebook paths before job creation.
    """
    try:
        w.workspace.get_status(path)
        return True
    except Exception:
        return False


def join_repo_path(base: str, rel: str) -> str:
    """
    Join a repository base path and a relative notebook path.
    - base: optional directory prefix inside the repo ('' means repo root)
    - rel: relative path like 'save_jobs__ops_excellence/get_job_details'
    """
    base_clean = (base or "").strip().strip("/")
    rel_clean = (rel or "").strip().lstrip("/")
    return f"{base_clean}/{rel_clean}" if base_clean else rel_clean


def build_tasks(source_mode: str, nb1_path: str, nb2_path: str, env: str):
    """
    Build the two notebook tasks:
    - GetJobDetails (first)
    - OpsRunData_Inc (depends_on GetJobDetails)
    The 'source_mode' should be 'WORKSPACE' or 'GIT'.
    """
    t_get = jobs.Task(
        task_key="GetJobDetails",
        notebook_task=jobs.NotebookTask(
            notebook_path=nb1_path,
            source=jobs.Source(source_mode),
            base_parameters={"ENV": env, "exclude_job_ids": ""}
        ),
        timeout_seconds=0
    )
    t_inc = jobs.Task(
        task_key="OpsRunData_Inc",
        depends_on=[jobs.TaskDependency(task_key="GetJobDetails")],
        notebook_task=jobs.NotebookTask(
            notebook_path=nb2_path,
            source=jobs.Source(source_mode),
            base_parameters={"ENV": env}
        ),
        timeout_seconds=0
    )
    return [t_get, t_inc]


def create_job_with_fallback(
    w,
    job_name: str,
    tasks,
    emails_on_failure,
    git_source_obj=None,
    source_mode: str = "WORKSPACE",
    nb1_path: str = "",
    nb2_path: str = "",
    env: str = "prod"
) -> int:
    """
    Create the two-task job with a serverless-first strategy, falling back to a small job cluster if needed.
    - tasks: built via build_tasks()
    - git_source_obj: pass for GIT mode, otherwise None
    - env: passed to notebook base_parameters
    Returns: job_id
    """
    try:
        for t in tasks:
            # Prefer serverless compute using SDK dataclass if available
            try:
                t.compute = jobs.ComputeSpec(kind="SERVERLESS")
            except Exception:
                # If the SDK version lacks ComputeSpec, skip and rely on fallback
                pass
        created = w.jobs.create(
            name=job_name,
            max_concurrent_runs=1,
            tasks=tasks,
            git_source=git_source_obj,
            email_notifications=jobs.JobEmailNotifications(
                on_failure=emails_on_failure,
                no_alert_for_skipped_runs=True
            ) if emails_on_failure else None
        )
        print(f"Job created ({source_mode}, serverless): {created.job_id}")
        return created.job_id
    except Exception as e:
        print(f"[WARN] Serverless creation failed, falling back to job cluster via SDK: {e}")
        # Assign a small job cluster to each task
        for t in tasks:
            t.job_cluster_key = "oe-cluster"
        created = w.jobs.create(
            name=job_name,
            max_concurrent_runs=1,
            git_source=git_source_obj,
            job_clusters=[
                jobs.JobCluster(
                    job_cluster_key="oe-cluster",
                    new_cluster=jobs.NewCluster(
                        spark_version="14.3.x-scala2.12",
                        node_type_id="Standard_D4ds_v5",
                        num_workers=1,
                        runtime_engine=jobs.RuntimeEngine.STANDARD,
                        enable_elastic_disk=True,
                    ),
                )
            ],
            tasks=tasks,
            email_notifications=jobs.JobEmailNotifications(
                on_failure=emails_on_failure,
                no_alert_for_skipped_runs=True
            ) if emails_on_failure else None
        )
        print(f"Job created ({source_mode}, job cluster): {created.job_id}")
        return created.job_id


