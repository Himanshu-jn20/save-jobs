# Databricks notebook source
# DBTITLE 1,Library Install
# %pip install --upgrade databricks-sdk
%pip install databricks-sdk==0.52.0
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Operational Excellence Config Runner
# MAGIC %run ./config

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,imports
import json
import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType
from functools import reduce
from operator import add


# COMMAND ----------

def get_workspace_creds(workspace):
  host = spark.conf.get("spark.databricks.workspaceUrl")
  return host


def get_workspace_client():
  # host, scope, key = get_workspace_creds(workspace)
  
  # return WorkspaceClient(
  #   host  = host,
  #   token = dbutils.secrets.get(scope = scope, key = key)
  # )
  return WorkspaceClient()


# COMMAND ----------

def table_exists(table_name):
    query = f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA} LIKE '{table_name}'"
    result = spark.sql(query)
    return result.count() > 0

# COMMAND ----------

# DBTITLE 1,Active Workspace Job Monitor
def get_job_ids_to_monitor():
  if table_exists(f"job_run_details"):
    df = spark.sql(f'''select distinct job_id from {UC_CATALOG}.{UC_SCHEMA}.job_details where is_job_deleted = 'no'
                union 
                 select distinct job_id from {UC_CATALOG}.{UC_SCHEMA}.job_run_details where job_state__life_cycle_state in ("PENDING","RUNNING","TERMINATING")
                ''')
  else:
    df = spark.sql(f'''select distinct job_id from {UC_CATALOG}.{UC_SCHEMA}.job_details where is_job_deleted = 'no'
                ''')
  job_ids_in_workspace = [row['job_id'] for row in df.collect()]  

  print(f"[INFO] Discovered {len(job_ids_in_workspace)} job_ids")
  return list(set(job_ids_in_workspace))

# COMMAND ----------

# DBTITLE 1,Job Run Data Collector
def get_job_run_data(w, job_ids):
  print(f"[INFO] Fetching runs serially for {len(job_ids)} jobs ...")
  run_data = []
  for job_id in job_ids:
    job_start_time = spark.sql(f'''select coalesce(max(job_start_time),0) as max_job_start_time 
                                from {UC_CATALOG}.{UC_SCHEMA}.job_run_details where job_id = {job_id} and 
                                job_state__life_cycle_state in ("TERMINATED","INTERNAL_ERROR") 
                                ''').first()['max_job_start_time']
    job_run_data = [c.as_dict() for c in w.jobs.list_runs(job_id=job_id, expand_tasks=True, start_time_from=job_start_time)]
    # job_run_data = [{**run, 'workspace': 'logfood'} for run in job_run_data]
    run_data.extend(job_run_data)
  print(f"[INFO] Retrieved {len(run_data)} run records.")
  return run_data

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.ERROR)

def fetch_job_run_data(w, job_id, start_time):
    try:
        return [c.as_dict() for c in w.jobs.list_runs(job_id=job_id, expand_tasks=True, start_time_from=start_time)]
    except Exception as e:
        logging.error(f"Error fetching job run data for job_id={job_id}: {e}")
        return []

def get_job_run_data_parallel(w, job_ids):
    print(f"[INFO] Fetching runs in parallel for {len(job_ids)} jobs ...")
    job_start_time_map = {}
    if table_exists(f"job_run_details"):
        job_ids_str = ','.join(map(str, job_ids))
        query = f'''
            SELECT job_id, COALESCE(MAX(job_start_time), 0) AS max_job_start_time
            FROM {UC_CATALOG}.{UC_SCHEMA}.job_run_details
            WHERE job_id IN ({job_ids_str})
              AND job_state__life_cycle_state IN ("TERMINATED", "INTERNAL_ERROR")
            GROUP BY job_id
        '''
        job_start_times = spark.sql(query).collect()
        job_start_time_map = {row['job_id']: row['max_job_start_time'] for row in job_start_times}
        print(f"[INFO] Built start_time map for {len(job_start_time_map)} jobs.")

    run_data = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_job_run_data, w, job_id, job_start_time_map.get(job_id, 0)) for job_id in job_ids]
        for future in futures:
            try:
                run_data.extend(future.result())
            except Exception as e:
                logging.error(f"Error processing future: {e}")
                continue

    print(f"[INFO] Retrieved {len(run_data)} run records (parallel).")
    return run_data

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

def preprocess_run_data(run_data):
    required_fields = {
        'cleanup_duration': 'job_cleanup_duration',
        'creator_user_name': 'job_creator_user_name',
        'end_time': 'job_end_time',
        'execution_duration': 'job_execution_duration',
        'job_id': 'job_id',
        'job_clusters': 'job_job_clusters',
        'job_parameters': 'job_job_parameters',
        'number_in_job': 'job_number_in_job',
        'original_attempt_run_id': 'job_original_attempt_run_id',
        'overriding_parameters': 'job_overriding_parameters',
        'run_duration': 'job_run_duration',
        'run_id': 'job_run_id',
        'run_name': 'job_run_name',
        'run_page_url': 'job_run_page_url',
        'run_type': 'job_run_type',
        'schedule': 'job_schedule',
        'setup_duration': 'job_setup_duration',
        'start_time': 'job_start_time',
        'state': {
            'life_cycle_state': 'job_state__life_cycle_state',
            'result_state': 'job_state__result_state',
            'state_message': 'job_state__state_message',
            'user_cancelled_or_timedout': 'job_state__user_cancelled_or_timedout'
        },
        'tasks': 'job_tasks',
        'trigger': 'job_trigger',
        'trigger_info': 'job_trigger_info'
    }
    
    relevant_task_keys = {'cleanup_duration', 'end_time', 'execution_duration', 'run_id', 'setup_duration','start_time'}  
    
    processed_data = []
    for item in run_data:
        processed_item = {}
        for key, value in required_fields.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    processed_item[sub_value] = item.get(key, {}).get(sub_key, None)
            elif key == 'tasks':
                tasks = item.get(key, [])
                filtered_tasks = [{k: v for k, v in task.items() if k in relevant_task_keys} for task in tasks]
                processed_item[value] = filtered_tasks
            else:
                processed_item[value] = item.get(key, None)
        processed_data.append(Row(**processed_item))
    
    return processed_data

schema = StructType([
    StructField("job_cleanup_duration", LongType(), True),
    StructField("job_creator_user_name", StringType(), True),
    StructField("job_end_time", LongType(), True),
    StructField("job_execution_duration", LongType(), True),
    StructField("job_id", LongType(), True),
    StructField("job_job_clusters", ArrayType(MapType(StringType(), StringType())), True),
    StructField("job_job_parameters", ArrayType(MapType(StringType(), StringType())), True),
    StructField("job_number_in_job", LongType(), True),
    StructField("job_original_attempt_run_id", LongType(), True),
    StructField("job_overriding_parameters", MapType(StringType(), MapType(StringType(), StringType())), True),
    StructField("job_run_duration", LongType(), True),
    StructField("job_run_id", LongType(), True),
    StructField("job_run_name", StringType(), True),
    StructField("job_run_page_url", StringType(), True),
    StructField("job_run_type", StringType(), True),
    StructField("job_schedule", MapType(StringType(), StringType()), True),
    StructField("job_setup_duration", LongType(), True),
    StructField("job_start_time", LongType(), True),
    StructField("job_state__life_cycle_state", StringType(), True),
    StructField("job_state__result_state", StringType(), True),
    StructField("job_state__state_message", StringType(), True),
    StructField("job_state__user_cancelled_or_timedout", StringType(), True),
    StructField("job_tasks", ArrayType(MapType(StringType(), LongType())), True),
    StructField("job_trigger", StringType(), True),
    StructField("job_trigger_info", MapType(StringType(), LongType()), True)
])

# COMMAND ----------

def create_job_run_df(run_data):
  job_run_df = spark.createDataFrame(preprocess_run_data(run_data), schema)
  try:
    print(f"[INFO] job_run_df rows: {job_run_df.count()}")
  except Exception:
    pass
  return job_run_df

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType, BooleanType

# def preprocess_task_run_data(run_data):
#     required_fields = {
#         'attempt_number': 'task_attempt_number',
#         'cleanup_duration': 'task_cleanup_duration',
#         'cluster_instance': 'task_cluster_instance',
#         'end_time': 'task_end_time',
#         'execution_duration': 'task_execution_duration',
#         'git_source': 'task_git_source',
#         'job_run_id': 'task_job_run_id',
#         'notebook_task': 'task_notebook_task',
#         'run_id': 'task_run_id',
#         'run_if': 'task_run_if',
#         'setup_duration': 'task_setup_duration',
#         'start_time': 'task_start_time',
#         'key': 'task_key',
#         'existing_cluster_id': 'task_existing_cluster_id',
#         'depends_on': 'task_depends_on',
#         'libraries': 'task_libraries',
#         'description': 'task_description',
#         'state': {
#             'life_cycle_state': 'task_state__life_cycle_state',
#             'result_state': 'task_state__result_state',
#             'state_message': 'task_state__state_message',
#             'user_cancelled_or_timedout': 'task_state__user_cancelled_or_timedout',
#         },
#         'notebook_output': 'task_notebook_output',
#         'error': 'task_error',
#         'error_trace': 'task_error_trace',
#         'sql_task': 'task_sql_task',
#         'original_attempt_run_id': 'task_original_attempt_run_id',
#         'condition_task': 'task_condition_task',
#         'run_job_task': 'task_run_job_task',
#         'queue_duration': 'task_queue_duration',
#         'pipeline_task': 'task_pipeline_task'
#     }
    
#     processed_data = []
#     for item in run_data:
#         processed_item = {}
#         for key, value in required_fields.items():
#             if isinstance(value, dict):
#                 for sub_key, sub_value in value.items():
#                     processed_item[sub_value] = item.get(key, {}).get(sub_key, None)
#             else:
#                 processed_item[value] = item.get(key, None)
#         processed_data.append(Row(**processed_item))
    
#     return processed_data

task_schema = StructType([
    StructField("task_attempt_number", LongType(), True),
    StructField("task_cleanup_duration", LongType(), True),
    StructField("task_cluster_instance", MapType(StringType(), StringType()), True),
    StructField("task_end_time", LongType(), True),
    StructField("task_execution_duration", LongType(), True),
    StructField("task_git_source", MapType(StringType(), StringType()), True),
    StructField("task_job_run_id", LongType(), True),
    StructField("task_notebook_task", MapType(StringType(), StringType()), True),
    StructField("task_run_id", LongType(), True),
    StructField("task_run_if", StringType(), True),
    StructField("task_setup_duration", LongType(), True),
    StructField("task_start_time", LongType(), True),
    StructField("task_key", StringType(), True),

    StructField("task_existing_cluster_id", StringType(), True),
    StructField("task_depends_on", ArrayType(MapType(StringType(), StringType())), True),
    StructField("task_libraries", ArrayType(MapType(StringType(), StringType())), True),
    StructField("task_description", StringType(), True),
    StructField("task_state__life_cycle_state", StringType(), True),
    StructField("task_state__user_cancelled_or_timedout", StringType(), True),
    StructField("task_state__result_state", StringType(), True),
    StructField("task_state__state_message", StringType(), True),
    StructField("task_notebook_output", StringType(), True),
    StructField("task_error", StringType(), True),
    StructField("task_error_trace", StringType(), True),
    StructField("task_sql_task", MapType(StringType(), MapType(StringType(), StringType())), True),
    StructField("task_original_attempt_run_id", LongType(), True),
    StructField("task_condition_task", MapType(StringType(), StringType()), True),
    StructField("task_run_job_task", MapType(StringType(), LongType()), True),
    StructField("task_queue_duration", LongType(), True),
    StructField("task_pipeline_task", MapType(StringType(), BooleanType()), True)
])

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import Row

def process_item(item, required_fields):
  processed_item = {}
  for key, value in required_fields.items():
    if isinstance(value, dict):
        for sub_key, sub_value in value.items():
          processed_item[sub_value] = item.get(key, {}).get(sub_key, None)
    else:
      processed_item[value] = item.get(key, None)
  return Row(**processed_item)

def create_task_run_df_parallel(run_data):
  required_fields = {
    'attempt_number': 'task_attempt_number',
    'cleanup_duration': 'task_cleanup_duration',
    'cluster_instance': 'task_cluster_instance',
    'end_time': 'task_end_time',
    'execution_duration': 'task_execution_duration',
    'git_source': 'task_git_source',
    'job_run_id': 'task_job_run_id',
    'notebook_task': 'task_notebook_task',
    'run_id': 'task_run_id',
    'run_if': 'task_run_if',
    'setup_duration': 'task_setup_duration',
    'start_time': 'task_start_time',
    'task_key': 'task_key',
    'existing_cluster_id': 'task_existing_cluster_id',
    'depends_on': 'task_depends_on',
    'libraries': 'task_libraries',
    'description': 'task_description',
    'state': {
        'life_cycle_state': 'task_state__life_cycle_state',
        'user_cancelled_or_timedout': 'task_state__user_cancelled_or_timedout',
        'result_state': 'task_state__result_state',
        'state_message': 'task_state__state_message',
    },
    'notebook_output': 'task_notebook_output',
    'error': 'task_error',
    'error_trace': 'task_error_trace',
    'sql_task': 'task_sql_task',
    'original_attempt_run_id': 'task_original_attempt_run_id',
    'condition_task': 'task_condition_task',
    'run_job_task': 'task_run_job_task',
    'queue_duration': 'task_queue_duration',
    'pipeline_task': 'task_pipeline_task'
    }

  tasks = []
  for run in run_data:
    ltask = run['tasks']

    job_run_id = run['run_id']
    for task in ltask:
        task.get('notebook_task', {}).pop('base_parameters', None)
        task.pop('new_cluster', None)
        task.pop('run_job_task', None)
        task['job_run_id'] = job_run_id
          
    tasks.extend(ltask)

  processed_data = []
  with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_item, item, required_fields) for item in tasks]
    for future in as_completed(futures):
      processed_data.append(future.result())
  
  task_run_df = spark.createDataFrame(processed_data, task_schema)
  try:
    print(f"[INFO] task_run_df rows: {task_run_df.count()}")
  except Exception:
    pass
  return task_run_df

# COMMAND ----------

# DBTITLE 1,Task DataFrame Creation Function
# def create_task_run_df(workspace, run_data):
#   required_fields = {
#         'attempt_number': 'task_attempt_number',
#         'cleanup_duration': 'task_cleanup_duration',
#         'cluster_instance': 'task_cluster_instance',
#         'end_time': 'task_end_time',
#         'execution_duration': 'task_execution_duration',
#         'git_source': 'task_git_source',
#         'job_run_id': 'task_job_run_id',
#         'notebook_task': 'task_notebook_task',
#         'run_id': 'task_run_id',
#         'run_if': 'task_run_if',
#         'setup_duration': 'task_setup_duration',
#         'start_time': 'task_start_time',
#         'key': 'task_key',
#         'existing_cluster_id': 'task_existing_cluster_id',
#         'depends_on': 'task_depends_on',
#         'libraries': 'task_libraries',
#         'description': 'task_description',
#         'state': {
#             'life_cycle_state': 'task_state__life_cycle_state',
#             'user_cancelled_or_timedout': 'task_state__user_cancelled_or_timedout',
#             'result_state': 'task_state__result_state',
#             'state_message': 'task_state__state_message',  
#         },
#         'notebook_output': 'task_notebook_output',
#         'error': 'task_error',
#         'error_trace': 'task_error_trace',
#         'sql_task': 'task_sql_task',
#         'original_attempt_run_id': 'task_original_attempt_run_id',
#         'condition_task': 'task_condition_task',
#         'run_job_task': 'task_run_job_task',
#         'queue_duration': 'task_queue_duration',
#         'pipeline_task': 'task_pipeline_task'
#     }
  
#   processed_data = []
#   i=1
#   run_data_len = len(run_data)
#   for run in run_data:
#     print(f"processint {i} out of {run_data_len}")
#     i+=1
#     for item in run_data:
#         # Skipping Delta load tables for now.
#         if 'tasks' not in item:
#           continue
        
#         processed_item = {}
#         for key, value in required_fields.items():
#             if isinstance(value, dict):
#                 for sub_key, sub_value in value.items():
#                   for item2 in item['tasks']:
#                     processed_item[sub_value] = item2.get(key, {}).get(sub_key, None)
#             else:
#                 for item2 in item['tasks']:
#                     processed_item[value] = item2.get(key, None)
#         processed_data.append(Row(**processed_item))

#   task_run_df = spark.createDataFrame(processed_data, task_schema)
#   task_run_df = task_run_df.withColumn('workspace', lit(workspace))
#   return task_run_df

# COMMAND ----------

# DBTITLE 1,Failed Task Detail Extractor
def get_failed_task_details(w, task_run_df):
  failed_tasks = task_run_df.where("task_state__result_state = 'FAILED'").select('task_run_id').collect()
  
  if len(failed_tasks) == 0:
    return None
  
  run_dtl_data = []
  for run in failed_tasks:
    try:
      out = w.jobs.get_run_output(run_id=run['task_run_id']).as_dict()
    except Exception as e:
      error = str(e)
      out = {'metadata': {'run_id': run['task_run_id']},
            'notebook_output': f"{error}",
            'error': f"{error}",
            'error_trace': f"{error}",
            'original_attempt_run_id': None}
    run_dtl_data.append(out)

  schema = StructType([
    StructField("metadata", StructType([
        StructField("run_id", StringType(), nullable=True),
        StructField("original_attempt_run_id", StringType(), nullable=True)
    ]), nullable=True),
    StructField("notebook_output", StringType(), nullable=True),
    StructField("error", StringType(), nullable=True),
    StructField("error_trace", StringType(), nullable=True)
  ])
  df = spark.createDataFrame(run_dtl_data, schema)
  df = df.withColumn('error', coalesce(df.error, lit(None).cast('string'))) \
        .withColumn('error_trace', coalesce(df.error_trace, lit(None).cast('string'))) \
        .withColumn('notebook_output', coalesce(df.notebook_output, lit(None).cast('string')))
  df = df.selectExpr('metadata.run_id', 'cast(notebook_output as string) notebook_output', 'error', 'error_trace', 'metadata.original_attempt_run_id as original_attempt_run_id')
  return df
    

# COMMAND ----------

# spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

def merge_job_run_details(job_run_df):
    
    if table_exists(f"job_run_details"):
        print("[INFO] Merging data into job_run_details table...")
        target_table = DeltaTable.forName(spark, f"{UC_CATALOG}.{UC_SCHEMA}.job_run_details")
        try:
            before_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.job_run_details").count()
            print(f"[INFO] job_run_details rows before MERGE: {before_cnt}")
        except Exception:
            pass
        target_table.alias("target").merge(
            job_run_df.alias("source"),
            "source.job_run_id = target.job_run_id"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        print(f"[INFO] {UC_CATALOG}.{UC_SCHEMA}.job_run_details dataset have been updated successfully.")
    else:
        job_run_df.write.format("delta").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.job_run_details")
    target_table = DeltaTable.forName(spark, f"{UC_CATALOG}.{UC_SCHEMA}.job_run_details")
    try:
        after_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.job_run_details").count()
        print(f"[INFO] job_run_details rows after MERGE/CREATE: {after_cnt}")
    except Exception:
        pass
    display(target_table.toDF())

# COMMAND ----------

def merge_task_run_details():
  if table_exists("task_run_details"):
    try:
      print(f"[INFO] Merging data into task_run_details table...")
      try:
          before_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.task_run_details").count()
          print(f"[INFO] task_run_details rows before MERGE: {before_cnt}")
      except Exception:
          pass
      display(spark.sql(f'''MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.task_run_details target
                USING df_task_final_vw source
                ON source.task_run_id = target.task_run_id
                WHEN MATCHED THEN
                  UPDATE SET *
                WHEN NOT MATCHED THEN
                  INSERT *'''))
      print(f"[INFO] {UC_CATALOG}.{UC_SCHEMA}.task_run_details dataset have been updated successfully.")
      try:
          after_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.task_run_details").count()
          print(f"[INFO] task_run_details rows after MERGE: {after_cnt}")
      except Exception:
          pass
    except Exception as e:
      print(str(e))
      print(f"Merge operation on Task_Run_Details has failed. As part of the exception handling, the Rollback of {UC_CATALOG}.{UC_SCHEMA}.job_run_details table is being performed.")
      try:
        prevVersion = spark.sql(f'''SELECT max(version) -1 as previousVersion  FROM (DESCRIBE HISTORY {UC_CATALOG}.{UC_SCHEMA}.job_run_details) a  where operation = 'MERGE' ''').first()['previousVersion']
        spark.sql(f'RESTORE TABLE {UC_CATALOG}.{UC_SCHEMA}.job_run_details TO VERSION AS OF {prevVersion}')
        print(f"Rollback of {UC_CATALOG}.{UC_SCHEMA}.job_run_details to version {prevVersion} has finished successfully.")
        print("It's safe to repair/re-run the Job.")
      except Exception as e2:
        print(e2)
        print("Rollback operation didn't finish successfully. The job requires manual intervention to rollback Job_run_details table to previous version. Until then, please DO NOT RERUN the job.")
      raise e    
  else:
    # Create table if it doesn't exist using staged view
    try:
      print(f"[INFO] task_run_details table not found. Creating new table {UC_CATALOG}.{UC_SCHEMA}.task_run_details from df_task_final_vw ...")
      df_new = spark.table("df_task_final_vw")
      df_new.write.format("delta").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.task_run_details")
      created_cnt = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.task_run_details").count()
      print(f"[INFO] Created task_run_details with {created_cnt} rows.")
    except Exception as e:
      print(f"[ERROR] Unable to create {UC_CATALOG}.{UC_SCHEMA}.task_run_details from df_task_final_vw: {e}")
      raise

# COMMAND ----------

def update_original_attempt_run_id(w):
  # Extracting Task Original Attempt ID for any of failed Job Run IDs

  #get the minimum task start time of incremental dataset  
  min_task_start_time = spark.sql('select min(task_start_time) from df_task_final_vw where task_start_time!=0').first()[0]

  #get all the task run IDs of a failed run and which doesn't have original_attempt_run_id 
  org_failed_tasks = spark.sql(f'''
                              with failed_run_ids as (
                              select distinct TASK_JOB_RUN_ID as JOB_RUN_ID from {UC_CATALOG}.{UC_SCHEMA}.task_run_details 
                              where task_start_time>={min_task_start_time} and task_state__result_state in ('FAILED','TIMEDOUT','CANCELED')
                              )
                              select task_run_id from {UC_CATALOG}.{UC_SCHEMA}.task_run_details 
                              where task_job_run_id in (select JOB_RUN_ID from failed_run_ids ) and 
                              task_original_attempt_run_id is null
                            ''').collect()
  print(f"[INFO] Candidate failed task_run_ids missing original_attempt_run_id: {len(org_failed_tasks)}")
  if len(org_failed_tasks) > 0:
    org_run_data = []
    for run_org in org_failed_tasks:
      try:
        out_org = w.jobs.get_run_output(run_id=run_org['task_run_id']).as_dict()
        org_run_data.append({
          'run_id': out_org['metadata']['run_id'],
          'original_attempt_run_id': out_org['metadata']['original_attempt_run_id']
        })
      except Exception as e:
        print(e)
        failed_task_run_id = run_org['task_run_id']
        print(f"[INFO] original_attempt_run_id couldn't be extracted for task_run_id = {failed_task_run_id}")
        print('[INFO] This is not critical')
        pass
    if(len(org_run_data) == 0):
      print('[INFO] No original attempt run id could be extracted for any of the failed tasks')
      print('[INFO] This is not critical')
      return

    df_org_run = spark.createDataFrame(org_run_data)#.selectExpr('metadata.run_id', 'metadata.original_attempt_run_id')
    df_org_run.createOrReplaceTempView('org_run_vw')
    print(f'''[INFO] Updating original_attempt_run_id for all the tasks of all the failed job runs for which original_attempt_run_id was extracted succesfully.
          This is required for identifying between a repair and retry run.
          ''')
    display(spark.sql(f'''MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.task_run_details target
                      USING org_run_vw source
                      ON source.run_id = target.task_run_id and target.task_original_attempt_run_id is NULL
                      WHEN MATCHED THEN
                        UPDATE SET task_original_attempt_run_id = source.original_attempt_run_id
                      '''
                      )
            )
    print(f'[INFO] Updation of original_attempt_run_id in {UC_CATALOG}.{UC_SCHEMA}.task_run_details is completed.''')
    

# COMMAND ----------

# DBTITLE 1,Task Run Monitoring System
def ops_run_data__inc_load():
  w = get_workspace_client()
  job_ids = get_job_ids_to_monitor()
  if not job_ids:
    print("[INFO] No job ids discovered; exiting.")
    dbutils.notebook.exit("NO_JOBS")
  run_data = get_job_run_data_parallel(w, job_ids)
  if not run_data:
    print("[INFO] No run_data fetched from Jobs API; exiting.")
    dbutils.notebook.exit("NO_RUNS")
  job_run_df = create_job_run_df(run_data)
  task_run_df = create_task_run_df_parallel(run_data)
  failed_task_run_df = get_failed_task_details(w, task_run_df)

  if(failed_task_run_df is None):
    df_task_final = task_run_df.select(
      "*"
      # ,lit(None).cast("string").alias("task_notebook_output")
      # ,lit(None).cast("string").alias("task_error")
      # ,lit(None).cast("string").alias("task_error_trace")
      # ,lit(None).alias("task_original_attempt_run_id")
    )
  else:
    df_task_final = task_run_df.join(failed_task_run_df, on=task_run_df.task_run_id == failed_task_run_df.run_id,how='left').select(
      task_run_df["task_attempt_number"],
      task_run_df["task_cleanup_duration"],
      task_run_df["task_cluster_instance"],
      task_run_df["task_end_time"],
      task_run_df["task_execution_duration"],
      task_run_df["task_git_source"],
      task_run_df["task_job_run_id"],
      task_run_df["task_notebook_task"],
      task_run_df["task_run_id"],
      task_run_df["task_run_if"],
      task_run_df["task_setup_duration"],
      task_run_df["task_start_time"],
      task_run_df["task_key"],
      task_run_df["task_existing_cluster_id"],
      task_run_df["task_depends_on"],
      task_run_df["task_libraries"],
      task_run_df["task_description"],
      task_run_df["task_state__life_cycle_state"],
      task_run_df["task_state__user_cancelled_or_timedout"],
      task_run_df["task_state__result_state"],
      task_run_df["task_state__state_message"],
      failed_task_run_df.notebook_output.alias("task_notebook_output"),
      failed_task_run_df.error.alias("task_error"),
      failed_task_run_df.error_trace.alias("task_error_trace"),
      failed_task_run_df.original_attempt_run_id.alias("task_original_attempt_run_id"),
      task_run_df["task_sql_task"],
      task_run_df["task_condition_task"],
      task_run_df["task_run_job_task"],
      task_run_df["task_queue_duration"],
      task_run_df["task_pipeline_task"]
    )

  # job_run_df.createOrReplaceTempView('job_run_df_vw')
  merge_job_run_details(job_run_df)
  if not table_exists("task_run_details"):
      df_task_final.write.format("delta").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.task_run_details")

  df_task_final.createOrReplaceTempView('df_task_final_vw')
  merge_task_run_details()

  update_original_attempt_run_id(w)



# COMMAND ----------

ops_run_data__inc_load()

# COMMAND ----------
