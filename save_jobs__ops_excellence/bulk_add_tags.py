# Databricks notebook source
# DBTITLE 1,Install Required Libraries
%pip install databricks-sdk==0.52.0
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Bulk Tag Helper
# MAGIC Apply standard tags to multiple Jobs at once.
# MAGIC - Edit the Inputs cell below:
# MAGIC   - Set `TAGS` with keys you want to apply (leave values empty to skip)
# MAGIC   - Provide `JOB_IDS` as a list of integers
# MAGIC   - Choose `DRY_RUN` (preview only) and `OVERWRITE` (replace existing tag values)
# MAGIC - Run once with `DRY_RUN=True` to review, then set `DRY_RUN=False` to apply.

# COMMAND ----------

# DBTITLE 1,Inputs (set here)
# How to use:
# 1) Fill TAGS below (leave any key as "" to skip that tag)
# 2) Provide JOB_IDS as a Python list of integers
# 3) Choose DRY_RUN (preview only) and OVERWRITE (replace existing values) from widgets
# 4) Run the notebook. You can repeat with different TAGS/JOB_IDS for separate groups
# Provide tags to apply (leave any key as "" to skip)
TAGS = {
  "Team": "",
  "Type": "",
  "Owner": "",
  "Domain": "",
  "Priority": "",
  "Project": ""
}

# Provide job IDs to update
JOB_IDS = [
  # 12345, 67890
]

# Execution options
# Widgets for convenience; you can still edit defaults below if running outside notebook
try:
    dbutils.widgets.dropdown("DRY_RUN", "true", ["true","false"], "DRY_RUN (preview only; no changes)")
    dbutils.widgets.dropdown("OVERWRITE", "true", ["true","false"], "OVERWRITE (replace existing tag values)")
    DRY_RUN = dbutils.widgets.get("DRY_RUN").lower().strip() == "true"
    OVERWRITE = dbutils.widgets.get("OVERWRITE").lower().strip() == "true"
except Exception:
    DRY_RUN = True       # If True, shows planned updates only
    OVERWRITE = True     # If False, preserves existing values

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.sdk import WorkspaceClient

ALLOWED_KEYS = ["Team","Type","Owner","Domain","Priority","Project"]

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Validate Inputs
jobs_to_update = []
for jid in JOB_IDS:
    try:
        jobs_to_update.append(int(jid))
    except Exception:
        print(f"[WARN] Skipping invalid job_id: {jid}")
tags_to_apply = {k: (v or "").strip() for k, v in TAGS.items() if k in ALLOWED_KEYS and (v or "").strip() != ""}
if len(tags_to_apply) == 0:
    print("[INFO] No tag values provided, nothing to apply.")
if len(jobs_to_update) == 0:
    dbutils.notebook.exit("No valid job IDs to process.")
print(f"[INFO] Mode: {'DRY RUN (preview only)' if DRY_RUN else 'APPLY CHANGES'}; Overwrite existing values: {OVERWRITE}")
print(f"[INFO] Tags to apply: {tags_to_apply}")
print(f"[INFO] Number of jobs to update: {len(jobs_to_update)}")

# COMMAND ----------

# DBTITLE 1,Apply Tags
results = []
for job_id in jobs_to_update:
    try:
        job = w.jobs.get(job_id=job_id).as_dict()
        settings = job.get("settings", {}) or {}
        tags = settings.get("tags", {}) or {}
        changes = {}
        for key in ALLOWED_KEYS:
            new_val = tags_to_apply.get(key, "")
            if new_val == "":
                continue
            if key in tags and not OVERWRITE:
                continue
            old_val = tags.get(key)
            if old_val != new_val:
                changes[key] = {"old": old_val, "new": new_val}
                tags[key] = new_val
        if len(changes) == 0:
            results.append({"job_id": job_id, "status": "no_change"})
            continue
        settings["tags"] = tags
        if DRY_RUN:
            results.append({"job_id": job_id, "status": "dry_run", "changes": changes})
            continue
        w.jobs.update(job_id=job_id, new_settings=settings)
        results.append({"job_id": job_id, "status": "updated", "changes": changes})
    except Exception as e:
        results.append({"job_id": job_id, "status": f"error: {e}"})
        continue

# COMMAND ----------

# DBTITLE 1,Summary
num_updated = sum(1 for r in results if r["status"] == "updated")
num_dry = sum(1 for r in results if r["status"] == "dry_run")
num_nochange = sum(1 for r in results if r["status"] == "no_change")
num_errors = sum(1 for r in results if str(r["status"]).startswith("error"))
print(f"Done. Updated={num_updated}, DryRun={num_dry}, NoChange={num_nochange}, Errors={num_errors}")
display(spark.createDataFrame([{"job_id": r["job_id"], "status": r["status"], "changes": str(r.get("changes"))} for r in results]))


