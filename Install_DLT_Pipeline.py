# Databricks notebook source
# MAGIC %pip install databricks_cli

# COMMAND ----------

# MAGIC %md
# MAGIC #### This notebook will install two DLT pipelines
# MAGIC One continously streaming and sessionizing incoming events. Another that can be scheduled to run daily to compute aggregates.
# MAGIC 
# MAGIC Make note of the pipeline IDs as you will want to use that with the DLT_Event_Log_Analysis (if you wish to build Data Quality related dashboards)

# COMMAND ----------

# DBTITLE 1,Adjust this value to avoid conflicts. It will be the Pipeline that you see in "Workflows"->"Delta Live Tables"
CONTINUOUS_PIPELINE_NAME = "CraigLukasikDLT_STREAMING"
BATCH_PIPELINE_NAME = "CraigLukasikDLT_BATCH"
TARGET_SCHEMA = "craig_lukasik_dlt_demo"

# COMMAND ----------

from databricks_cli.sdk.service import DeltaPipelinesService
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config

pipeline_service = DeltaPipelinesService(_get_api_client(get_config()))

# COMMAND ----------

# DBTITLE 1,Build the path to the DLT notebooks by using the path of this notebook.
nb_path = (
  dbutils.entry_point.getDbutils().notebook().getContext()
    .notebookPath().getOrElse(None)
    .replace("Install_DLT_Pipeline", "DLT-Sessionize-Stream-and-Batch-Aggregate")
)

# COMMAND ----------

# DBTITLE 1,Use Databricks API to register and start the Continuous DLT Pipeline
retval = pipeline_service.create(
  name=CONTINUOUS_PIPELINE_NAME, 
  target=TARGET_SCHEMA,
  development=True, 
  continuous=True, 
  libraries=[
    {
      "notebook": {
        "path": nb_path
      }
    }
  ],
  configuration = {
      "stream_and_aggregate": "false",
      "only_aggregate": "false"
  }  
)

# COMMAND ----------

print(retval)

# COMMAND ----------

# DBTITLE 1,Use Databricks API to register and start the Batch DLT Pipeline
retval = pipeline_service.create(
  name=BATCH_PIPELINE_NAME, 
  target=TARGET_SCHEMA,
  development=True, 
  continuous=False, 
  libraries=[
    {
      "notebook": {
        "path": nb_path
      }
    }
  ],
  configuration = {
      "stream_and_aggregate": "false",
      "only_aggregate": "true"
  }  
)

# COMMAND ----------

print(retval)
