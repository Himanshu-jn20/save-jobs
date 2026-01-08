# Databricks notebook source
# MAGIC %md
# MAGIC # Operational Excellence - Setup Artifacts
# MAGIC
# MAGIC Guided setup flow:
# MAGIC 1) Select ENV and map to target UC (catalog & schema); ensure they exist
# MAGIC 2) (Optional) Tag jobs or use the bulk helper
# MAGIC 3) Create the two-task pipeline job and run it once
# MAGIC 4) Create the analytics view (`ops_run_details_vw`)
# MAGIC 5) Create the Lakeview dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Environment and UC mapping (PROD/DEV)
# MAGIC Provide PROD/DEV UC targets. The ENV dropdown selects which pair to use.

# COMMAND ----------

# MAGIC %md
# MAGIC Set the following widgets before proceeding:
# MAGIC - ENV: choose prod or dev (drives UC targets from `config.py`)
# MAGIC - TASK_SOURCE: select WORKSPACE or GIT (For Loader job creation)
# MAGIC - EMAILS_ON_FAILURE (required): single email for job failure alerts

# COMMAND ----------

# %pip install --upgrade databricks-sdk
%pip install databricks-sdk==0.52.0
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("EMAILS_ON_FAILURE", "", "Job failure email (required)")
dbutils.widgets.dropdown("ENV", "prod", ["prod","dev"], "Environment")
dbutils.widgets.dropdown("TASK_SOURCE", "WORKSPACE", ["WORKSPACE","GIT"], "Task source")

_email_single = dbutils.widgets.get("EMAILS_ON_FAILURE").strip()
EMAILS_ON_FAILURE = [_email_single] if _email_single else []
ENV = dbutils.widgets.get("ENV").lower().strip()
TASK_SOURCE = dbutils.widgets.get("TASK_SOURCE").strip().upper()
JOB_NAME = "JobRunDetails_Loader"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load config and resolve target UC from ENV
# MAGIC Ensure you've set PROD/DEV mappings in `save_jobs__ops_excellence/config.py` as per the README. ENV determines which mapping is used.

# COMMAND ----------

# DBTITLE 1,load config
# MAGIC %run ./config

# COMMAND ----------


# Validate UC mappings for the selected ENV only
if ENV == "prod":
    try:
        _ = PROD_CATALOG; _ = PROD_SCHEMA
    except NameError as e:
        raise Exception("Missing PROD_CATALOG/PROD_SCHEMA in save_jobs__ops_excellence/config.py. Please set these per README.") from e
    if (str(PROD_CATALOG).strip() == "") or (str(PROD_SCHEMA).strip() == ""):
        raise Exception("Empty PROD_CATALOG/PROD_SCHEMA in config.py. Set these per README.")
else:
    try:
        _ = DEV_CATALOG; _ = DEV_SCHEMA
    except NameError as e:
        raise Exception("Missing DEV_CATALOG/DEV_SCHEMA in save_jobs__ops_excellence/config.py. Please set these per README.") from e
    if (str(DEV_CATALOG).strip() == "") or (str(DEV_SCHEMA).strip() == ""):
        raise Exception("Empty DEV_CATALOG/DEV_SCHEMA in config.py. Set these per README and re-check after re-running the load config cell.")

CATALOG = str(PROD_CATALOG).strip() if ENV == "prod" else str(DEV_CATALOG).strip()
SCHEMA  = str(PROD_SCHEMA).strip()   if ENV == "prod" else str(DEV_SCHEMA).strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source inputs
# MAGIC - If `TASK_SOURCE=WORKSPACE`: set `WORKSPACE_DIR` to the repo folder that contains `save_jobs__ops_excellence/`
# MAGIC - If `TASK_SOURCE=GIT`: set `GIT_URL`, `GIT_BRANCH`, `GIT_PROVIDER`, and optionally `GIT_REPO_BASE_PATH` (path in the repo that contains `save_jobs__ops_excellence/`; can be empty if at repo root)

# COMMAND ----------

# Set these according to TASK_SOURCE
WORKSPACE_DIR = ""  # e.g., "/Repos/you/bse-data" (required when TASK_SOURCE=WORKSPACE)
GIT_URL = ""       # e.g., "https://github.com/your/repo" (required when TASK_SOURCE=GIT)
GIT_BRANCH = ""  # e.g., "master" (required when TASK_SOURCE=GIT)
GIT_PROVIDER = ""  # one of: gitHub, azureDevOpsServices, gitLab (required when TASK_SOURCE=GIT)
GIT_REPO_BASE_PATH = ""  # e.g., "src"; optional — empty string means notebooks are at repo root

# COMMAND ----------

# Validate inputs based on TASK_SOURCE
if TASK_SOURCE == "WORKSPACE":
    if not WORKSPACE_DIR.strip():
        displayHTML("""
        <div style="padding:8px;border:1px solid #ccc;background:#fff8e1">
          <b>Action required:</b> TASK_SOURCE=WORKSPACE. Set WORKSPACE_DIR (e.g., <code>"/Repos/you/bse-data"</code>) in the previous cell and re-run.
        </div>
        """)
        raise ValueError("TASK_SOURCE=WORKSPACE requires WORKSPACE_DIR (e.g., /Repos/you/bse-data). Set and re-run.")
elif TASK_SOURCE == "GIT":
    if not (GIT_URL.strip() and GIT_BRANCH.strip() and GIT_PROVIDER.strip()):
        displayHTML("""
        <div style="padding:8px;border:1px solid #ccc;background:#fff8e1">
          <b>Action required:</b> TASK_SOURCE=GIT. Set GIT_URL, GIT_BRANCH and GIT_PROVIDER in the previous cell (provider: gitHub, azureDevOpsServices, or gitLab) and re-run.
        </div>
        """)
        raise ValueError("TASK_SOURCE=GIT requires GIT_URL, GIT_BRANCH and GIT_PROVIDER. Set and re-run.")
else:
    raise ValueError("TASK_SOURCE must be WORKSPACE or GIT.")

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, Source, GitSource
from databricks.sdk.service.dashboards import Dashboard
import time
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC Load setup helpers

# COMMAND ----------

# MAGIC %run ./setup_artifacts_utils

# COMMAND ----------

# DBTITLE 1,Ensure UC Catalog/Schema

print(f"[INFO] Target UC: {CATALOG}.{SCHEMA} (ENV={ENV})")
db_exists = spark.catalog.databaseExists(f"{CATALOG}.{SCHEMA}")
if not db_exists:
    print(f"[INFO] Catalog/Schema {CATALOG}.{SCHEMA} does not exist. Attempting to create...")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
        print("[INFO] UC catalog/schema ensured.")
    except Exception as e:
        print(f"[ERROR] Unable to create or ensure UC objects: {e}")
        raise
else:
    print(f"[INFO] Catalog/Schema {CATALOG}.{SCHEMA} already exists.")

# Require failure email
if not EMAILS_ON_FAILURE:
    raise ValueError("EMAILS_ON_FAILURE is required. Please provide a single email address for failure alerts.")

# COMMAND ----------

# DBTITLE 1,Tagging reminder (optional bulk helper)
# MAGIC %md
# MAGIC ## 2) Tagging reminder (optional bulk helper)
# MAGIC Ensure your jobs have Team/Type (and optional Owner/Domain/Priority/Project) tags. You can bulk apply tags using the helper notebook.
# MAGIC
# MAGIC try:
# MAGIC     bulk_path = f"{REPO_PATH}/save_jobs__ops_excellence/bulk_add_tags" if REPO_PATH else "save_jobs__ops_excellence/bulk_add_tags"
# MAGIC     displayHTML(f"""
# MAGIC     <div>
# MAGIC       <p><b>Tagging reminder:</b> Ensure your jobs have Team/Type (and optional Owner/Domain/Priority/Project) tags applied.</p>
# MAGIC       <p>You can use the bulk helper notebook: <code>{bulk_path}</code></p>
# MAGIC     </div>
# MAGIC     """)
# MAGIC except Exception:
# MAGIC     pass

# COMMAND ----------

# DBTITLE 1,Create JobRunsDetails_Loader Job (Two-Task Pipeline)
# MAGIC %md
# MAGIC ## 3) Create pipeline job (attempt serverless, fallback to job cluster)
# MAGIC Creates two tasks (GetJobDetails → Incremental Load). Tries serverless; if not available, falls back to a small job cluster.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./setup_artifacts_utils

# COMMAND ----------

if TASK_SOURCE == "WORKSPACE" and not WORKSPACE_DIR:
    raise ValueError("Please set WORKSPACE_DIR (e.g., /Repos/you/bse-data) when TASK_SOURCE=WORKSPACE")

#create workspace client
w = WorkspaceClient()

nb1_rel = "save_jobs__ops_excellence/get_job_details"
nb2_rel = "save_jobs__ops_excellence/ops_run_data_inc_load"
# Workspace notebook paths
nb1_ws = f"{WORKSPACE_DIR}/{nb1_rel}" if WORKSPACE_DIR else nb1_rel
nb2_ws = f"{WORKSPACE_DIR}/{nb2_rel}" if WORKSPACE_DIR else nb2_rel
# Git notebook paths (prefix with optional base path)
nb1_git = join_repo_path(GIT_REPO_BASE_PATH, nb1_rel)
nb2_git = join_repo_path(GIT_REPO_BASE_PATH, nb2_rel)

# Workspace path presence warning
missing = []
for p in [nb1_ws, nb2_ws]:
    if not check_workspace_path(w, p):
        missing.append(p)
if missing and TASK_SOURCE == "WORKSPACE":
    print("[WARN] Some notebook paths were not found in the Workspace. Verify WORKSPACE_DIR or import notebooks:")
    for p in missing:
        print(f" - {p}")

# COMMAND ----------

# Helper: find an existing job by name from system.lakeflow.jobs to avoid creating duplicates
def _find_job_id_by_name(name: str) -> int | None:
    # Restrict lookup to the current workspace when available, and only latest non-deleted job versions
    try:
        current_workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    except Exception:
        current_workspace_id = ""
    ws_filter = f"workspace_id = '{current_workspace_id}'" if current_workspace_id else ""
    df = spark.sql(f"""
        WITH sj AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
            FROM system.lakeflow.jobs
            where {ws_filter}
            QUALIFY rn = 1
        )
        SELECT job_id
        FROM sj
        WHERE delete_time IS NULL
          AND LOWER(name) = LOWER('{name}')
        LIMIT 1
    """)
    result = df.collect()
    if result:
        return result[0]['job_id']
    return None

# COMMAND ----------

job_id = _find_job_id_by_name(JOB_NAME)
if job_id:
    print(f"[INFO] Found existing job '{JOB_NAME}' (ID: {job_id}) in the system.lakeflow.jobs table. Skipping creation.")
else:
    if TASK_SOURCE == "WORKSPACE":
        tasks = build_tasks("WORKSPACE", nb1_ws, nb2_ws, ENV)
        job_id = create_job_with_fallback(w, JOB_NAME, tasks, EMAILS_ON_FAILURE, None, "WORKSPACE", nb1_ws, nb2_ws, ENV)
    else:
        if not (GIT_URL and GIT_BRANCH and GIT_PROVIDER):
            raise ValueError("For TASK_SOURCE=GIT, please provide GIT_URL, GIT_BRANCH, and GIT_PROVIDER.")
        tasks = build_tasks("GIT", nb1_git, nb2_git, ENV)
        git_src = GitSource(git_url=GIT_URL, git_provider=GIT_PROVIDER, git_branch=GIT_BRANCH)
        job_id = create_job_with_fallback(w, JOB_NAME, tasks, EMAILS_ON_FAILURE, git_src, "GIT", nb1_git, nb2_git, ENV)

# COMMAND ----------

# Share job link and scheduling guidance
if job_id:
    print(f"[INFO] Job created with ID: {job_id}")
    # Build fully-qualified job URL using workspaceUrl from Spark conf
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        workspace_url = ""
    job_url = f"https://{workspace_url}/jobs/{job_id}" if workspace_url else f"/jobs/{job_id}"
    displayHTML(
        f'<div>Open job: <a href="{job_url}">{job_url}</a>. '
        f'Recommended schedule: every 30 minutes. Please configure the job schedule as needed.</div>'
    )

# COMMAND ----------

# DBTITLE 1,Trigger First Run and Poll Status
# MAGIC %md
# MAGIC ## 4) Trigger first run and wait for completion
# MAGIC Seeds initial tables and validates configuration.

# COMMAND ----------

run_success = False
run_id = None
if job_id:
    try:
        # Trigger and wait using SDK (single run)
        final = w.jobs.run_now_and_wait(job_id=job_id, job_parameters={"ENV": ENV}, timeout=datetime.timedelta(minutes=45))
        # Pull run_id and status from the completed run
        run_id = getattr(final, "run_id", None)
        state = getattr(final, "state", None) or {}
        result = getattr(state, "result_state", None) if not isinstance(state, dict) else state.get("result_state")
        run_success = result is not None and "SUCCESS" in str(result).upper()
        print(f"[RESULT] First run result_state={result}")
    except Exception as e:
        print(f"[RUN] Failed to trigger or poll job run: {e}")

# COMMAND ----------

# DBTITLE 1,Create ops_run_details_vw (same catalog/schema)
# MAGIC %md
# MAGIC ## 5) Create ops_run_details_vw (same catalog/schema)
# MAGIC Creates curated view for reporting. No-op if it exists.
# MAGIC
# MAGIC %md
# MAGIC Creating the view for the selected ENV using:
# MAGIC - CATALOG: {CATALOG}
# MAGIC - SCHEMA: {SCHEMA}
# MAGIC These are derived from PROD/DEV in config.py based on the ENV widget.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./create_view__ops_run_details_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Create AI/BI dashboard (prefix with selected UC)
# MAGIC Loads the dashboard template, replaces catalog/schema, and creates the AI/BI dashboard using the SDK. If creation fails, we fall back to writing a JSON for manual import.

# COMMAND ----------

# Load the dashboard template from the new canonical filename
template_file = "SaveJobs__Ops_Excellence__template.lvdash.json"
try:
    with open(template_file, "r", encoding="utf-8") as f:
        template_text = f.read()
except Exception:
    nb_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    nb_dir = os.path.dirname(nb_path)
    candidate = f"/Workspace/{nb_dir}/{template_file}"
    with open(candidate, "r", encoding="utf-8") as f:
        template_text = f.read()

src_prefix = "[catalog].[schema]."
dst_prefix = f"{CATALOG}.{SCHEMA}."
updated_text = template_text.replace(src_prefix, dst_prefix)
# Also replace standalone placeholders if present
updated_text = updated_text.replace("[catalog]", CATALOG).replace("[schema]", SCHEMA)
# Replace failure alert email placeholder if provided
try:
    # Treat EMAILS_ON_FAILURE as a single email string from the widget
    email_str = dbutils.widgets.get("EMAILS_ON_FAILURE").strip()
    if email_str:
        updated_text = updated_text.replace("[failure_email_alert@email.com]", email_str)
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC Set your SQL Warehouse details:
# MAGIC - WAREHOUSE_ID: the target SQL Warehouse ID used by the dashboard

# COMMAND ----------

# Set your SQL Warehouse ID here (required)
WAREHOUSE_ID = ""

# COMMAND ----------

w = WorkspaceClient()
dashboard_id = None
create_errors = []
if not str(WAREHOUSE_ID).strip():
    raise ValueError("Please set WAREHOUSE_ID (SQL Warehouse ID) before creating the dashboard.")

try:
    created = w.lakeview.create(
        dashboard=Dashboard(
            display_name="SaveJobs__Ops_Excellence",
            warehouse_id=WAREHOUSE_ID,
            serialized_dashboard=updated_text
        )
    )
    dashboard_id = created.dashboard_id
except Exception as e:
    create_errors.append(str(e))

if dashboard_id:
    print(f"AI/BI dashboard created with ID: {dashboard_id}")
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    displayHTML(f"<div>Draft Dashboard created. Open AI/BI and search for <b>SaveJobs__Ops_Excellence</b> or ID <code>{dashboard_id}</code>. "
                f'Link: <a href="/sql/dashboardsv3/{dashboard_id}">/sql/dashboardsv3/{dashboard_id}</a></div>')
    print("Please validate and Publish.")
else:
    output_dbfs_path = "dbfs:/FileStore/SaveJobs__Ops_Excellence.lvdash.json"
    output_files_href = "/files/SaveJobs__Ops_Excellence.lvdash.json"
    dbutils.fs.put(output_dbfs_path, updated_text, True)
    print("Automatic dashboard creation failed; wrote JSON to DBFS for manual upload.")
    print("Errors:", create_errors)
    displayHTML(f'<div>Download and upload to AI/BI: <a href="{output_files_href}" download>SaveJobs__Ops_Excellence.lvdash.json</a></div>')

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 7) Summary
# MAGIC Dashboard, job, first run result, and view status.

# COMMAND ----------

if dashboard_id:
    displayHTML(f"""
    <ul>
      <li>Dashboard: SaveJobs__ops_excellence (ID: {dashboard_id})</li>
      <li>Job name: {JOB_NAME if JOB_NAME else "JobRunsDetails_Loader"} {f"(ID: {job_id})" if job_id else ""}</li>
      <li>First run: {'SUCCESS' if run_success else 'NOT SUCCESSFUL/UNKNOWN'} {f'(run_id: {run_id})' if run_id else ''}</li>
      <li>View: {CATALOG}.{SCHEMA}.ops_run_details_vw created (if missing)</li>
    </ul>
    """)
else:
    displayHTML(f"""
    <ul>
      <li>Dashboard JSON written to DBFS for manual import (see message above)</li>
      <li>Job name: {JOB_NAME if JOB_NAME else "JobRunsDetails_Loader"} {f"(ID: {job_id})" if job_id else ""}</li>
      <li>First run: {'SUCCESS' if run_success else 'NOT SUCCESSFUL/UNKNOWN'} {f'(run_id: {run_id})' if run_id else ''}</li>
      <li>View: {CATALOG}.{SCHEMA}.ops_run_details_vw created (if missing)</li>
    </ul>
    """)

