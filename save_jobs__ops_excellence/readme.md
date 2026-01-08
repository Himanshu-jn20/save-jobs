# SaveJobs : 

SaveJobs (Dashboard) is a lightweight, opinionated data product for consolidated reporting and analysis of Databricks Jobs. It automatically discovers production jobs using Team–Type tags, loads run and task telemetry into Unity Catalog Delta tables, and presents a dimensional model for flexible analysis.

## Dimensions and Tagging

You can slice and analyze Databricks Jobs using the following tags:

- **Team** – Who owns the job. (Mandatory for job discovery)
- **Type** – Which environment the job runs in (for example, production or staging). (Mandatory for job discovery)
- **Owner** – Who to page or contact for the job.
- **Domain** – The associated business domain.
- **Priority** – Criticality level (P1–P4).
- **Project** – Related initiative or program.

Notes:
- Team and Type are required to discover jobs. Jobs that do not have both will not be monitored.
- The set of tag columns captured in `job_details` is driven by `REQUIRED_TAG_KEYS` and `ADDITIONAL_TAG_KEYS` in `save_jobs__ops_excellence/config.py`. By default, `REQUIRED_TAG_KEYS` includes Team, Type, Owner, Domain, Priority, Project, and `ADDITIONAL_TAG_KEYS` is empty (you can add more as needed).
- If you add new tags to `ADDITIONAL_TAG_KEYS`, those columns will appear in the `job_details` table. You must update the AI/BI dashboard to reference any new tag columns you introduce.

## Out-of-the-Box Datasets

SaveJobs creates detailed datasets in your target schema to support downstream reporting and observability.

- `job_details` – Job configuration and metadata.
- `job_run_details` – Execution and performance information for each job run.
- `task_run_details` – Task-level telemetry for fine-grained analysis.
- `ops_run_details_vw` – Curated view that joins key job and run tables with normalized timestamps, durations, and commonly used fields.

Datasets are built using Databricks system tables (`system.lakeflow.jobs`) and Databricks SDK (Jobs API) and maintained via a scheduled pipeline.

## AI/BI Dashboard

SaveJobs ships with an AI/BI dashboard that uses Unity Catalog tables and views as its underlying datasets. The dashboard is organized into pages to minimize scrolling and improve readability.

### Jobs – “Know what you own”

- Inventory of jobs with ownership metadata (Team, Owner).
- Contextual attributes such as Domain, Priority, and Project.

### Job Runs – “How are the jobs doing?”

- Top failed jobs over a selected time window across dimensions such as Team, Domain, Project, and Priority.
- Failure-over-time trends.
- Recurring errors, including surfaced error messages.

### On-call – “Support”

- Live view of running jobs, highlighting long-running executions based on a dynamic threshold of 2× the average duration over the last 30 days.
- Status of the latest run for each job to support rapid triage.
- Flags for adherence to expected production standards - scheduled via Git, running as a Service Principal, and failure alert configuration in place.

---

### Quickstart
0) Import notebooks
   - If using WORKSPACE: copy the `save_jobs__ops_excellence` folder into your workspace (e.g., `/Repos/you/your-repo/save_jobs__ops_excellence` or another workspace path). In `save_jobs__ops_excellence/setup_artifacts.py`, set `TASK_SOURCE=WORKSPACE` and `WORKSPACE_DIR` to the parent folder containing `save_jobs__ops_excellence/`.
   - If using GIT: add the `save_jobs__ops_excellence` folder to your Git repository. 
1) Configure `save_jobs__ops_excellence/config.py`:
   - Set per-environment UC mapping (PROD/DEV → `UC_CATALOG`, `UC_SCHEMA`) driven by the ENV widget
   - Configure tag capture:
     - `REQUIRED_TAG_KEYS`: defaults to `["Team", "Type", "Owner", "Domain", "Priority", "Project"]`
       - Team and Type are mandatory for discovery and should not be removed
     - `ADDITIONAL_TAG_KEYS`: defaults to `[]` (add your own keys as needed)
   - Provide your Team–Type pairs as `TEAM_TYPE_PAIRS`, e.g.:
     `[{"team":"IT_DATA","type":"PROD"}, {"team":"GTM","type":"PROD"}]`
2) Provision artifacts:
   - Open `save_jobs__ops_excellence/setup_artifacts.py`
   - Set widgets:
    - ENV (prod/dev)
     - TASK_SOURCE (WORKSPACE/GIT)
     - EMAILS_ON_FAILURE (single email, required)
   - In the “Source inputs” cell, set:
     - If WORKSPACE: `WORKSPACE_DIR` (e.g., `/Repos/you/bse-data`)
     - If GIT: `GIT_URL`, `GIT_BRANCH`, `GIT_PROVIDER`, optional `GIT_REPO_BASE_PATH`
   - In the dashboard section, set `WAREHOUSE_ID` (SQL Warehouse)
   - Run the notebook to create the two-task job (`JobRunDetails_Loader`) 
   - The setup notebook triggers the first pipeline run and creates `ops_run_details_vw`.
   - The notebook then creates the AI/BI dashboard `SaveJobs__ops_excellence`.
4) Open the dashboard:
   - In Databricks AI/BI, search for `SaveJobs__ops_excellence` (or use the link printed by the setup notebook)

---

### Prerequisites
- Databricks workspace
- Unity Catalog enabled; permission to create objects in your target catalog and schema
- Access to `system.lakeflow.jobs` system table
- Databricks Runtime with Spark and Delta (standard DBR works)
- A SQL warehouse

---

### Preflight checklist
- Privileges on UC schema: ability to CREATE/ALTER in `{UC_CATALOG}.{UC_SCHEMA}`
- Access to `system.lakeflow.jobs`
- A cluster or serverless availability; recommended DBR ≥ 13.x
- Jobs API permissions to create/update jobs (for setup notebook)

---

### Tagging Convention (important)
Add the following tags to Databricks jobs you want to monitor:
- Team: your team name (e.g., IT_Data) — mandatory for discovery
- Type: environment indicator (e.g., PROD) — mandatory for discovery

Defaults for analytics (captured in `job_details` via `REQUIRED_TAG_KEYS`):
- Owner
- Domain
- Priority
- Project

You can add more tags by listing them in `ADDITIONAL_TAG_KEYS` in `config.py`. Any new tags will be captured in `job_details` as `job__tags__{TagName}` columns. If you add new tags, update the AI/BI dashboard to include them where relevant.

Only these tags are stored in `job_details` for simplicity and portability.

---

### Tagging Guide 
Use these standards when adding tags to your Databricks Jobs (adapt to your org as needed):

- Mandatory ( Used to identify production jobs to be monitored)
  - Team: Team owning the job (e.g., IT-Data).
  - Type: Job environment. Allowed values: PROD, DEV, STG, UAT. 
- Recommended (used for dimentional reporting and analytics)
  - Owner: Email of the on-call/owner (e.g., xyz@domain.com)
  - Domain: Example - Consumption, Finance, GTM, Other, Salesforce, Training, People etc. 
  - Priority: P1/P2/P3/P4. Definitions:
    - P1: Critical for external customers/multiple teams (respond within 1 hour)
    - P2: Critical for a team (respond within 4 hours)
    - P3: Moderate (respond within 24 hours)
    - P4: Not critical (best effort)
  - Project: Project name (e.g., ops_excellence)

Operational standards to consider:
- Add Job failure email alert ( email): xyz@domain.com


---

### Configure
Edit `save_jobs__ops_excellence/config.py`:
```
# Set Per-environment UC targets
PROD_CATALOG ex. "main"
PROD_SCHEMA ex. "ops_excellence"
DEV_CATALOG ex. "integration"
DEV_SCHEMA ex. "ops_excellence"

# ENV is read from the notebook widget (prod/dev) to select targets

# Team–Type pairs for discovery (case-insensitive)
Example :
TEAM_TYPE_PAIRS = [
  {"team":"IT_DATA","type":"PROD"}  
]
```

The view and tables are created as:
- `{UC_CATALOG}.{UC_SCHEMA}.job_details`
- `{UC_CATALOG}.{UC_SCHEMA}.job_run_details`
- `{UC_CATALOG}.{UC_SCHEMA}.task_run_details`
- `{UC_CATALOG}.{UC_SCHEMA}.ops_run_details_vw`


---

### Source options (WORKSPACE vs GIT) for JobRunsLoader job creation
- WORKSPACE: Set `WORKSPACE_DIR` to the repo folder containing `save_jobs__ops_excellence/` (e.g., `/Repos/you/bse-data`).
- GIT: Set `GIT_URL`, `GIT_BRANCH`, `GIT_PROVIDER`, and optionally `GIT_REPO_BASE_PATH` (empty means notebooks at repo root). The setup will create a job with `source="GIT"` and repo-relative notebook paths.

### Dashboard creation details
- The setup notebook uses the template `save_jobs__ops_excellence/SaveJobs__Ops_Excellence__template.lvdash.json`.
- It replaces the `"[catalog].[schema]."` placeholders with your selected `{UC_CATALOG}.{UC_SCHEMA}` and creates an AI/BI dashboard.
- Set `WAREHOUSE_ID` (SQL Warehouse) in the dashboard section before creation. If creation fails, a JSON is written to DBFS for manual import.
- If you add tag keys via `ADDITIONAL_TAG_KEYS`, modify the AI/BI dashboard to surface the new columns as needed (they are available in `job_details`).

---

### Bulk tag helper (one-time or repeatable)
Use `save_jobs__ops_excellence/bulk_add_tags.py` to apply the six standard tags to many jobs. Edit the “Inputs (set here)” cell:
- Set TAGS (any keys left as "" are skipped)
- Provide JOB_IDS as a Python list of ints
- Set DRY_RUN and OVERWRITE
Example:
```python
TAGS = {
  "Team": "IT_Data",
  "Type": "PROD",
  "Owner": "user@company.com",
  "Domain": "Finance",
  "Priority": "P2",
  "Project": "rev_ops"
}
JOB_IDS = [12345, 67890]
DRY_RUN = True
OVERWRITE = True
```
Run with DRY_RUN=True to preview; then set DRY_RUN=False to apply. Repeat with different TAGS/JOB_IDS as needed for separate groups.

---

### Scheduling recommendations
- Schedule the pipeline (incremental loader) every 30 minutes (or your preferred frequency)
- Re-create the view after schema changes only (Job 3 is usually one-time)

---

### Validations
- Check that jobs are tagged with Team and Type matching your configured pairs
- Confirm tables exist:
```
SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.job_details LIMIT 10;
SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.job_run_details LIMIT 10;
SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.task_run_details LIMIT 10;
SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.ops_run_details_vw LIMIT 10;
```

---

### Troubleshooting
- No jobs discovered: verify tags and env; confirm access to `system.lakeflow.jobs`
- Dashboard empty: run both loader notebooks; check row counts (see Validations)
- Jobs API errors: reduce concurrency, ensure permissions; if serverless unsupported, attach a cluster manually

---

### SaveJobs adds minimal overhead to your workspace:
- Storage:  Almost negligible, often just a fraction of a cent per month.
- Compute: One Incremental loader run on Serverless costs: < 1-2 DBU, 20 cents. 
- Dashboard: Per-query AI/BI costs, typically cents per dashboard load.

---

### FAQ
- Can I add more tags? Yes—extend `TAG_KEYS_ALLOWED` in `config.py` and add columns to the schema in `get_job_details.py`.
- Can I switch catalog/schema later? Yes—update `config.py` and re-run the notebooks; remember to update the dashboard via `setup_artifacts.py` or manually.


