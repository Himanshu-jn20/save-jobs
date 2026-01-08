# Databricks notebook source
# DBTITLE 1,Install Required Libraries
# %pip install --upgrade databricks-sdk
%pip install databricks-sdk==0.52.0
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Environment
dbutils.widgets.dropdown("ENV", "prod", ["prod","dev"], "Environment")

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./config

# COMMAND ----------

# DBTITLE 1,Resolve UC target from ENV
# Validate and set UC_CATALOG/UC_SCHEMA based on ENV and config mappings
try:
    env_val = dbutils.widgets.get("ENV").lower().strip()
except Exception:
    raise Exception("ENV widget is required. Please set the ENV widget before running this notebook.")
if env_val == "prod":
    try:
        _ = PROD_CATALOG; _ = PROD_SCHEMA  # type: ignore
    except Exception as e:
        raise Exception("Missing PROD_CATALOG/PROD_SCHEMA in save_jobs__ops_excellence/config.py. Please set these per README.") from e
    if (str(PROD_CATALOG).strip() == "") or (str(PROD_SCHEMA).strip() == ""):  # type: ignore
        raise Exception("Empty PROD_CATALOG/PROD_SCHEMA in config.py. Set these per README.")
    UC_CATALOG = PROD_CATALOG  # type: ignore
    UC_SCHEMA = PROD_SCHEMA    # type: ignore
else:
    try:
        _ = DEV_CATALOG; _ = DEV_SCHEMA  # type: ignore
    except Exception as e:
        raise Exception("Missing DEV_CATALOG/DEV_SCHEMA in save_jobs__ops_excellence/config.py. Please set these per README.") from e
    if (str(DEV_CATALOG).strip() == "") or (str(DEV_SCHEMA).strip() == ""):  # type: ignore
        raise Exception("Empty DEV_CATALOG/DEV_SCHEMA in config.py. Set these per README.")
    UC_CATALOG = DEV_CATALOG  # type: ignore
    UC_SCHEMA = DEV_SCHEMA    # type: ignore

# COMMAND ----------

# DBTITLE 1,Delta Optimization Settings
# For Serverless Jobs, set Delta optimization/compaction/statistics configs via cluster policy or workspace defaults.

# COMMAND ----------

# DBTITLE 1,Preflight Checks (Catalog/Schema, System Tables Access)
print(f"[INFO] ENV={env_val} -> UC target: {UC_CATALOG}.{UC_SCHEMA}")
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
    # lightweight probe for system table access
    spark.sql("SELECT 1 FROM system.lakeflow.jobs LIMIT 1").collect()
    print("[INFO] Preflight checks passed (catalog/schema ensured, system tables accessible).")
except Exception as e:
    print(f"[Preflight] Warning: environment check reported: {e}")
    print("[Preflight] Ensure you have CREATE on target UC schema and access to system.lakeflow.jobs.")

# COMMAND ----------

# DBTITLE 1,Exclude Job IDs (Optional)
dbutils.widgets.text("exclude_job_ids", "", "List of jobs to exclude (comma separated)")
exclude_job_ids_str = dbutils.widgets.get("exclude_job_ids")
exclude_job_ids_list = [jid.strip() for jid in exclude_job_ids_str.split(",") if jid.strip()]
exclude_job_ids_sql_str = ",".join([f"'{jid}'" for jid in exclude_job_ids_list])
print(exclude_job_ids_sql_str)

if len(exclude_job_ids_list) != 0:
    sql_filter = f'sj.job_id not in ({exclude_job_ids_sql_str})'
else:
    sql_filter = '1=1'

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import col, trim, lower, upper, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    ArrayType,
    MapType,
)

from concurrent.futures import ThreadPoolExecutor
import time
import random
try:
    # When executed as a module (tests), import from config.py
    from .config import TEAM_TYPE_PAIRS, TAG_KEYS_ALLOWED, UC_CATALOG, UC_SCHEMA
except Exception:
    # When run as a Databricks notebook with %run ./config, variables are already in scope
    pass


def _table_exists(table_name: str) -> bool:
    """
    Returns True if a table with the given name exists in the configured catalog and schema.
    Uses SHOW TABLES for a lightweight existence check.
    """
    res = spark.sql(f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA} LIKE '{table_name}'")
    return res.count() > 0


def _get_workspace_client() -> WorkspaceClient:
    # If running on a Databricks cluster, auth is handled automatically.
    return WorkspaceClient()


def _get_jobs_by_pairs():
    """
    Discover Databricks jobs from system.lakeflow.jobs that match mandatory Team and Type tags.
    - Only the latest (QUALIFY rn=1) version of each job is considered
    - Filters out soft-deleted jobs (delete_time IS NULL)
    - Restricts to current workspace_id when available from Spark conf
    """
    # Build a small pairs dataframe and join for exact Team–Type combinations
    try:
        pairs = [{"team": p["team"].upper().strip(), "type": p["type"].upper().strip()} for p in TEAM_TYPE_PAIRS]
    except Exception:
        pairs = []
    if len(pairs) == 0:
        print("[INFO] No TEAM_TYPE_PAIRS configured. Skipping job discovery.")
        return []
    pairs_df = spark.createDataFrame(pairs)
    pairs_df.createOrReplaceTempView("pairs_vw")
    # Resolve current workspace_id from Spark conf (serverless-safe)
    try:
        workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    except Exception:
        workspace_id = ""
    ws_filter = f"sj.workspace_id = '{workspace_id}'" if workspace_id else ""
    print(f"[INFO] Discovering jobs for pairs={pairs} workspace_id={workspace_id or '<unknown>'}")
    rows = spark.sql(
        f"""
        WITH sj AS (
          SELECT *,
                 ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
          FROM system.lakeflow.jobs
          where {ws_filter}
          QUALIFY rn = 1
        )
        SELECT job_id
        FROM sj
        JOIN pairs_vw p
          ON upper(sj.tags.Team) = p.team
         AND upper(sj.tags.Type) = p.type
        WHERE delete_time IS NULL
          AND {sql_filter}
        """
    ).collect()
    job_ids = [r["job_id"] for r in rows]
    print(f"[INFO] Discovered {len(job_ids)} job_ids matching Team/Type in this workspace.")
    return job_ids


def _tags_filter_inplace(job_dict: dict) -> None:
    """
    Keep only allowed tag keys in-place (Team, Type, Owner, Domain, Priority, Project).
    This trims noisy/unrelated tags to maintain a predictable schema for job_details.
    """
    settings = job_dict.get("settings", {})
    tags = settings.get("tags", {})
    to_delete = [k for k in list(tags.keys()) if k not in TAG_KEYS_ALLOWED]
    for k in to_delete:
        del tags[k]


def _get_job_details_payload(job_ids: list[int]) -> list[dict]:
    """
    Retrieve job details via Databricks Jobs API for the given job_ids.
    - Parallel requests with retries and jitter to handle transient API issues.
    - Adds is_job_deleted='no' and trims tags using _tags_filter_inplace.
    """
    if not job_ids:
        return []
    w = _get_workspace_client()
    print(f"[INFO] Fetching job details via SDK for {len(job_ids)} jobs...")
    def _fetch_single(job_id: int, max_retries: int = 3):
        for attempt in range(max_retries + 1):
            try:
                return w.jobs.get(job_id=job_id).as_dict()
            except Exception:
                if attempt == max_retries:
                    return None
                time.sleep((2 ** attempt) + random.uniform(0, 1))
        return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(lambda jid: _fetch_single(jid), job_ids))
    job_data = [r for r in results if r is not None]
    print(f"[INFO] Retrieved {len(job_data)} job payloads (failures={len(results) - len(job_data)}).")
    for jd in job_data:
        jd["is_job_deleted"] = "no"
        _tags_filter_inplace(jd)
    return job_data


def _schema_job_details() -> StructType:
    """
    Schema capturing core job fields plus only the requested tag set.
    The tag columns are generated dynamically from TAG_KEYS_ALLOWED in config.py.
    - Tag columns are prefixed with job__tags__{Key}
    - Schedule- and git-related columns are included for dashboarding
    """
    try:
        allowed_keys = list(TAG_KEYS_ALLOWED)  # type: ignore[name-defined]
    except Exception:
        allowed_keys = ["Team", "Type", "Owner", "Domain", "Priority", "Project"]
    # stable ordering for schema determinism
    allowed_keys = sorted(allowed_keys)

    fields: list[StructField] = [
        StructField("job_id", LongType(), True),
        StructField("created_time", LongType(), True),
        StructField("creator_user_name", StringType(), True),
        StructField("run_as_user_name", StringType(), True),
        StructField("is_job_deleted", StringType(), True),
        StructField("job__name", StringType(), True),
        StructField("job__schedule__pause_status", StringType(), True),
        StructField("job__schedule__quartz_cron_expression", StringType(), True),
        StructField("job__schedule__timezone_id", StringType(), True),
    ]
    # dynamic tag fields
    for key in allowed_keys:
        fields.append(StructField(f"job__tags__{key}", StringType(), True))
    # convenience and additional fields
    fields.extend(
        [
            StructField("job__timeout_seconds", LongType(), True),
            StructField("job__git_source__git_branch", StringType(), True),
            StructField("job__git_source__git_provider", StringType(), True),
            StructField("job__git_source__git_url", StringType(), True),
            StructField("job__email_notifications__on_failure", ArrayType(StringType()), True),
            StructField("job__email_notifications__on_success", ArrayType(StringType()), True),
            StructField("job__job_clusters", ArrayType(MapType(StringType(), StringType())), True),
            StructField("Job_link", StringType(), True),
        ]
    )
    return StructType(fields)


def _extract_value(d: dict, path: list[str]):
    """
    Safely traverse a nested dict by a list-based path. Returns None if any hop is missing.
    """
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _transform_job_data(job_payload: list[dict]) -> list[dict]:
    """
    Convert Jobs API JSON into a flat dict matching _schema_job_details.
    Adds a Job_link from the current workspace host if job_id is present.
    """
    if not job_payload:
        return []
    w = _get_workspace_client()
    # Prefer workspace URL from Spark conf for consistent UI links
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

    try:
        allowed_keys = list(TAG_KEYS_ALLOWED)  # type: ignore[name-defined]
    except Exception:
        allowed_keys = ["Team", "Type", "Owner", "Domain", "Priority", "Project"]
    allowed_keys = sorted(allowed_keys)

    out = []
    for job in job_payload:
        base = {
            "job_id": job.get("job_id"),
            "created_time": job.get("created_time"),
            "creator_user_name": job.get("creator_user_name"),
            "run_as_user_name": job.get("run_as_user_name"),
            "is_job_deleted": job.get("is_job_deleted"),
            "job__name": _extract_value(job, ["settings", "name"]),
            "job__schedule__pause_status": _extract_value(job, ["settings", "schedule", "pause_status"]),
            "job__schedule__quartz_cron_expression": _extract_value(job, ["settings", "schedule", "quartz_cron_expression"]),
            "job__schedule__timezone_id": _extract_value(job, ["settings", "schedule", "timezone_id"]),
            "job__timeout_seconds": _extract_value(job, ["settings", "timeout_seconds"]),
            "job__git_source__git_branch": _extract_value(job, ["settings", "git_source", "git_branch"]),
            "job__git_source__git_provider": _extract_value(job, ["settings", "git_source", "git_provider"]),
            "job__git_source__git_url": _extract_value(job, ["settings", "git_source", "git_url"]),
            "job__email_notifications__on_failure": _extract_value(job, ["settings", "email_notifications", "on_failure"]),
            "job__email_notifications__on_success": _extract_value(job, ["settings", "email_notifications", "on_success"]),
            "job__job_clusters": _extract_value(job, ["settings", "job_clusters"]),
            "Job_link": (f"https://{workspace_url}/jobs/{job.get('job_id')}" if (workspace_url and job.get("job_id")) else None),
        }
        # dynamic tag mapping
        for key in allowed_keys:
            base[f"job__tags__{key}"] = _extract_value(job, ["settings", "tags", key])
        out.append(base)
    return out


def write_job_details_table() -> None:
    """
    Discover jobs by tags and upsert into UC job_details table.
    Creates the table if absent. Idempotent for first-run safety.
    Steps:
    1) Discover job_ids from system.lakeflow.jobs using Team/Type
    2) Pull Jobs API payload in parallel with retries
    3) Normalize tag case (lower for most, upper for Priority)
    4) First run: saveAsTable; Subsequent runs: MERGE on job_id and soft-delete missing jobs
    """
    job_ids = _get_jobs_by_pairs()
    payload = _get_job_details_payload(job_ids)
    schema = _schema_job_details()
    df = spark.createDataFrame(_transform_job_data(payload), schema=schema)
    print(f"[INFO] Staged job_details rows: {df.count()}")
    try:
        sample = df.select("job_id", "job__name", "job__tags__Team", "job__tags__Type").limit(10).collect()
        if sample:
            print("[INFO] Sample job_details rows (up to 10):")
            for r in sample:
                print(f"  job_id={r['job_id']} name={r['job__name']} team={r['job__tags__Team']} type={r['job__tags__Type']}")
    except Exception:
        pass

    # Case normalization similar to ENG reference:
    # - Lowercase for most tag columns and Domain
    # - Uppercase for Priority
    # This avoids BI filters being split by case variants.
    try:
        allowed_keys = list(TAG_KEYS_ALLOWED)  # type: ignore[name-defined]
    except Exception:
        allowed_keys = ["Team", "Type", "Owner", "Domain", "Priority", "Project"]
    tag_cols_lower = [f"job__tags__{k}" for k in allowed_keys if k != "Priority"]
    tag_cols_upper = ["job__tags__Priority"] if "Priority" in allowed_keys else []
    for c in tag_cols_lower:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNotNull(), trim(lower(col(c)))).otherwise(col(c)))
    for c in tag_cols_upper:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNotNull(), trim(upper(col(c)))).otherwise(col(c)))
    is_empty = df.limit(1).count() == 0
    if is_empty:
        raise Exception("No matching jobs discovered with provided Team/Type tags for this workspace. Ensure jobs are tagged correctly and TEAM_TYPE_PAIRS in config.py is set.")
    df.createOrReplaceTempView("job_details_os_stage_vw")

    if _table_exists("job_details"):
        try:
            before_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.job_details").count()
            print(f"[INFO] Existing job_details rows before MERGE: {before_cnt}")
        except Exception:
            pass
        spark.sql(
            f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.job_details AS target
            USING job_details_os_stage_vw AS source
            ON target.job_id = source.job_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        try:
            after_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.job_details").count()
            print(f"[INFO] job_details rows after MERGE: {after_cnt}")
        except Exception:
            pass
        # Mark missing jobs as deleted
        spark.sql(
            f"""
            UPDATE {UC_CATALOG}.{UC_SCHEMA}.job_details
            SET is_job_deleted = 'yes'
            WHERE is_job_deleted != 'yes'
              AND job_id NOT IN (
                  SELECT job_id FROM job_details_os_stage_vw
              )
            """
        )
    else:
        # First run: create via DataFrame write using the explicit schema
        df.write.format("delta").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.job_details")

# COMMAND ----------

# DBTITLE 1,Run Loader
try:
    write_job_details_table()
    print(f"job_details upsert completed in {UC_CATALOG}.{UC_SCHEMA}.job_details")
except Exception as e:
    print(f"Error while loading job_details: {e}")
    raise


