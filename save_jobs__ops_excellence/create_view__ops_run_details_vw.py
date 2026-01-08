# Databricks notebook source
# DBTITLE 1,Install Required Libraries
# (No external libs required for view creation)

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook creates the `ops_run_details_vw` view.
# MAGIC - If run from `setup_artifacts.py`, it will use the parent-provided `CATALOG` and `SCHEMA` (resolved from ENV and config.py).

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./config

# COMMAND ----------

def create_ops_run_details_view() -> None:
    # Resolve target catalog/schema:
    # Preference order:
    # 1) CATALOG/SCHEMA variables from parent (e.g., setup_artifacts.py)
    # 2) UC_CATALOG/UC_SCHEMA from config.py
    try:
        target_catalog = CATALOG.strip()  # type: ignore # provided by parent
        target_schema = SCHEMA.strip()    # type: ignore # provided by parent
    except Exception:
        target_catalog = UC_CATALOG  # from config.py
        target_schema = UC_SCHEMA    # from config.py

    spark.sql(
        f"""
        CREATE VIEW IF NOT EXISTS {target_catalog}.{target_schema}.ops_run_details_vw AS
        select
          task_run_id,
          job_run_id,
          job_id,
          job__name,
          job_run_page_url,
          timestamp_millis(
            case
              when task_start_time = 0 then NULL
              else task_start_time
            end
          ) as task_start_time,
          timestamp_millis(
            case
              when task_end_time = 0 then NULL
              else task_end_time
            end
          ) as task_end_time,
          timestamp_millis(
            case
              when job_start_time = 0 then NULL
              else job_start_time
            end
          ) as job_start_time,
          timestamp_millis(
            case
              when job_end_time = 0 then NULL
              else job_end_time
            end
          ) as job_end_time,
          timestamp_millis(
            case
              when created_time = 0 then NULL
              else created_time
            end
          ) as created_time,
          floor(
            (
              case
                when
                  coalesce(job_run_duration, 0) = 0
                then
                  (job_setup_duration + job_execution_duration + job_cleanup_duration)
                else job_run_duration
              end
            )
            / 1000
          ) as job_elapsed_time_sec,
          floor(
            (task_setup_duration + task_execution_duration + task_cleanup_duration) / 1000
          ) as task_elapsed_time_sec,
          floor(task_cleanup_duration / 1000) as task_cleanup_duration_sec,
          floor(task_execution_duration / 1000) as task_execution_duration_sec,
          floor(task_setup_duration / 1000) as task_setup_duration_sec,
          floor(job_cleanup_duration / 1000) as job_cleanup_duration_sec,
          floor(job_execution_duration / 1000) as job_execution_duration_sec,
          floor(job_run_duration / 1000) as job_run_duration_sec,
          floor(job_setup_duration / 1000) as job_setup_duration_sec,
          task_attempt_number,
          task_cluster_instance,
          task_git_source,
          task_job_run_id,
          task_notebook_task,
          task_run_if,
          task_key,
          task_existing_cluster_id,
          task_depends_on,
          task_libraries,
          task_description,
          task_state__life_cycle_state,
          task_state__user_cancelled_or_timedout,
          task_state__result_state,
          task_state__state_message,
          task_notebook_output,
          task_error,
          task_error_trace,
          task_sql_task,
          task_original_attempt_run_id,
          job_creator_user_name,
          job_job_clusters,
          job_number_in_job,
          job_original_attempt_run_id,
          job_run_name,
          job_run_type,
          job_schedule,
          job_tasks,
          job_trigger,
          job_overriding_parameters,
          job_state__life_cycle_state,
          job_state__user_cancelled_or_timedout,
          job_state__result_state,
          job_state__state_message,
          job_job_parameters,
          creator_user_name,
          run_as_user_name,
          job__email_notifications__on_failure,
          job__email_notifications__on_success,
          job__schedule__pause_status,
          job__schedule__quartz_cron_expression,
          job__schedule__timezone_id,
          job__timeout_seconds,
          job__git_source__git_branch,
          job__git_source__git_provider,
          job__git_source__git_url,
          Job_link,
          job__tags__Team,
          job__tags__Type,
          job__tags__Owner,
          job__tags__Project,
          job__tags__Domain,
          job__tags__Priority
        from
          {target_catalog}.{target_schema}.task_run_details tr
    join {target_catalog}.{target_schema}.job_run_details jr
      on tr.task_job_run_id = jr.job_run_id
    join {target_catalog}.{target_schema}.job_details jd using (job_id)
        """
    )

# COMMAND ----------

try:
    create_ops_run_details_view()
    try:
        print(f"View created: {CATALOG}.{SCHEMA}.ops_run_details_vw")  # type: ignore
    except Exception:
        print(f"View created: {UC_CATALOG}.{UC_SCHEMA}.ops_run_details_vw")
except Exception as e:
    print(f"Error creating view: {e}")
    raise


