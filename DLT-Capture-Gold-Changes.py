# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

col_media_id = spark.conf.get("col_media_id", "media_id")
col_media_name = spark.conf.get("col_media_name", "media_name")
target = spark.conf.get("target", "craig_lukasik_dlt_demo")

# COMMAND ----------

@dlt.view
def agg_changes():
  return spark.readStream.format("delta") \
              .option("readChangeFeed", "true") \
    .table(f"{target}.gold_media_profile_counts")

# COMMAND ----------

dlt.create_streaming_live_table(name="gold_media_profile_counts_history")

# COMMAND ----------

dlt.apply_changes(
  target = "gold_media_profile_counts_history",
  source = "agg_changes",
  keys = [col_media_id, col_media_name],
  sequence_by = "_commit_version",
  ignore_null_updates = True,
  apply_as_deletes = col("_change_type") == "delete",
  column_list = [col_media_id, col_media_name, "unique_profiles", "_commit_timestamp"],
  stored_as_scd_type = 2
)
