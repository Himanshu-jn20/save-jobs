# Databricks notebook source
# This notebook holds configuration used by Operational Excellence loaders.
#
# How to use:
# - Optionally set a widget named "ENV" (values like "prod" or "dev") in a parent
#   notebook to switch UC targets dynamically:
#     dbutils.widgets.text("ENV", "prod")
#
# COMMAND ----------

"""
Runtime configuration for Operational Excellence (Open Source).

Provide your Unity Catalog target, workspace label, and tag values.
These are read by the loaders to discover and persist monitoring data.
"""

# COMMAND ----------
# MAGIC %md
# MAGIC Set Unity Catalog targets per environment:
# MAGIC - PROD_CATALOG / PROD_SCHEMA for production
# MAGIC - DEV_CATALOG / DEV_SCHEMA for development

# COMMAND ----------
PROD_CATALOG = ""
PROD_SCHEMA = ""
DEV_CATALOG = ""
DEV_SCHEMA = ""

# COMMAND ----------
# MAGIC %md
# MAGIC Select which Jobs to monitor by matching on their "Team" and "Type" tags.
# MAGIC - Comparison is case-insensitive in the loaders.
# MAGIC - Example: [{"team":"IT_DATA","type":"PROD"}, {"team":"GTM","type":"PROD"}]

# COMMAND ----------
# Team–Type pairs to select jobs to monitor (case-insensitive compare)
TEAM_TYPE_PAIRS = [
    {"team": "", "type": ""}  # Provide the Team and Type tags value of the Jobs to be monitored.
]

# Limit which tag keys from job settings are retained in job_details.settings.tags.
# Add or remove keys as needed for your reporting.
# Optional: limit tags that are captured into job_details.settings.tags
# Only the below tag keys are retained from job settings.
# Define required tag keys, then extend with additional keys as needed.
REQUIRED_TAG_KEYS = ["Team", "Type", "Owner", "Domain", "Priority", "Project"]
ADDITIONAL_TAG_KEYS = []  # Add more keys here (e.g., "Owner", "Domain", "Priority", "Project")
TAG_KEYS_ALLOWED = set(REQUIRED_TAG_KEYS) | set(ADDITIONAL_TAG_KEYS)

# COMMAND ----------
# Resolve active environment from a Databricks notebook widget if available.
# Defaults to "prod" when the widget is not defined.
# Resolve environment from notebook/parent notebook widget if available; default to 'prod'
_env = "prod"
try:
    _env = dbutils.widgets.get("ENV").lower().strip() or "prod"
except Exception:
    pass

# COMMAND ----------
# Derive the catalog and schema from the selected environment.
if _env == "prod":
    UC_CATALOG = PROD_CATALOG
    UC_SCHEMA = PROD_SCHEMA
else:
    UC_CATALOG = DEV_CATALOG
    UC_SCHEMA = DEV_SCHEMA

