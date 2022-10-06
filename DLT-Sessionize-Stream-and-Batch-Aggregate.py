# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### You can use this notebook to fulfill one of three flows:
# MAGIC 1. Stream and build the Bronze table & build the Aggregate (if `stream_and_aggregate` property is true). If you use this option, then you need only one DLT pipeline to accomplish the capturing of the raw events, transforming them into sessions, and building the final aggregate table.
# MAGIC 2. Stream and build the Bronze table only (if `stream_and_aggregate` property is false)
# MAGIC 3. Only build the aggregate table (if `only_aggregate` is true)
# MAGIC 
# MAGIC ### If you separate the stream and aggregate you will need two separate DLT pipelines
# MAGIC * The streaming one will be "Continuous" (set `stream_and_aggregate`=`false` and `only_aggregate`=`false`)
# MAGIC * The batch aggregate one should be set up as a daily job (set `only_aggregate`=`true`)
# MAGIC 
# MAGIC Note: If you set both `only_aggregate` and `stream_and_aggregate` to `true`, the `stream_and_aggregate` value will be ignored
# MAGIC 
# MAGIC ### Many properties are configurable
# MAGIC The cell titled `Properties from the DLT Pipeline "configuration" section of the settings` has a number of variables that can be set via the DLT Pipeline's Settings (under the "Configuration" section)

# COMMAND ----------

# DBTITLE 1,Properties from the DLT Pipeline "configuration" section of the settings
# Directory where raw events are landing. This program assumes we're working with CSV files.
data_dir = spark.conf.get("simulation_dir", "dbfs:/tmp/craig_lukasik_dlt_demo/raw_data")

# Spark schema for the raw event CSV files
event_schema = spark.conf.get("event_schema", 
  "timestamp timestamp, account_id string, profile_id string, media_id string, event_type string, ingest_date date")

# Amount of time we wait before a gap in a series of events closes a session
session_window_inactivity_period = spark.conf.get("session_window_inactivity_period", "1 hour")

# Stream with aggregate. If you want it all in one pipeline
stream_and_aggregate = (spark.conf.get("stream_and_aggregate", "true") == "true")
stream_and_aggregate_refresh_interval = spark.conf.get("stream_and_aggregate_refresh_interval", "24 hours")

# Only aggregate in the pipeline if true. This assumes another pipeline is capturing the event stream and sessionizing it
only_aggregate = (spark.conf.get("only_aggregate", "true") == "true")

# Database where media names map a media_id to a movie/show title
target = spark.conf.get("target", "craig_lukasik_dlt_demo")

# We are interested in 30 day aggregate
lookback_days = int(spark.conf.get("lookback_days", "30"))
                    
# Change to 0 if you want to consider data that has landed today
yesterday_days = int(spark.conf.get("yesterday_days", "1"))

# Set to false if you don't care about the historical aggregates
change_feed_enabled = spark.conf.get("change_feed_enabled", "true")
                     
# Column names
col_profile_id = spark.conf.get("col_profile_id", "profile_id")
col_timestamp = spark.conf.get("col_timestamp", "timestamp")
col_media_id = spark.conf.get("col_media_id", "media_id")
col_media_name = spark.conf.get("col_media_name", "media_name")
col_ingest_date = spark.conf.get("col_ingest_date", "ingest_date")

# Media table (the table that maps media_id to media_name)
tbl_media_names = spark.conf.get("tbl_media_names", "media_names")

# LIMIT for the number of media_id's we EXPECT within a session; helps to 
# ensure we don't blow up Spark due to the size of the collected set!
media_per_session_limit = spark.conf.get("media_per_session_limit", "100")

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw table with expectations
# MAGIC With expectations on a record, you can either 
# MAGIC 1. simply record the exception while still keeping the record flowing into the table
# MAGIC 2. drop the record
# MAGIC 3. fail the pipeline
# MAGIC 
# MAGIC We will show 1 and 2.
# MAGIC 
# MAGIC Read more here: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html

# COMMAND ----------

# DBTITLE 1,Bronze table with expectations
if not only_aggregate:
  raw_fake_data_dir = f"{data_dir}/"
  
  expect_and_keep = {
    f"valid {col_timestamp}": f"`{col_timestamp}` is not null",
    # A fake expectation for demonstration purposes: media_id column should not start with a "1"
    f"valid {col_media_id}": f"not ({col_media_id} like '1%')" 
  }

  expect_or_drop = {
    # A fake expectation for demonstration purposes: we don't watch TV on Mondays in this pretend universe.
    "valid day of week": f"date_format({col_ingest_date} ,'d') != 1"
  }
  @dlt.table(
    comment = "Raw event stream",
    table_properties={
      "quality": "bronze"
    } 
  )
  @dlt.expect_all(expect_and_keep)
  @dlt.expect_all_or_drop(expect_or_drop)
  def bronze_events():
    return spark.readStream.csv(
      path=f"{raw_fake_data_dir}", 
      header=True, 
      schema=event_schema
    )

# COMMAND ----------

# MAGIC %md
# MAGIC The silver table uses the `session_window` capabilities. For more details, read these:
# MAGIC * https://www.databricks.com/blog/2021/10/12/native-support-of-session-window-in-spark-structured-streaming.html
# MAGIC * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#types-of-time-windows
# MAGIC * https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.session_window.html

# COMMAND ----------

# DBTITLE 1,Silver aggregate view: sessions per profile, partitioned by end date
if not only_aggregate:
  # This table is not batched and will keep streaming!
  @dlt.table(
    partition_cols = ["session_end_date"],
    comment = f"Silver sessionized events with media_ids collected for each {col_profile_id}",
    table_properties={
      "quality": "silver"
    } 
  )
  @dlt.expect_or_drop("sanity check on the number of media items a profile viewed within a session",
                     f"size(media_id_set) <= {media_per_session_limit}", )
  def silver_sessions():
    df = (
      spark.readStream.table("LIVE.bronze_events")
        .groupBy(
          col(f"{col_profile_id}"), 
          session_window(col(f"{col_timestamp}"), session_window_inactivity_period)
        ).agg(collect_set(col(f"{col_media_id}")).alias("media_id_set"))
    )

    df = df.selectExpr(
      f"{col_profile_id}",
      "media_id_set",
      "session_window.start as session_start",
      "session_window.end as session_end",
      "to_date(session_window.end) as session_end_date"
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Everything below is enabled only if the flag `stream_and_aggregate` is `true` or the `only_aggregate` flag is `true`

# COMMAND ----------

if stream_and_aggregate or only_aggregate:
  @dlt.table(
    comment = f"Movie/show names that map to {col_media_id} values",
    table_properties={
      "quality": "gold"
    } 
  )
  def static_media_names():
    return spark.table(f"{target}.{tbl_media_names}")

# COMMAND ----------

if stream_and_aggregate or only_aggregate:
  @dlt.view(
    comment = "This is used to find the current latest session_end_date and the 30 day ago date",
  )
  def max_session_end():
    source_table = f"{target}.silver_sessions" if only_aggregate else "LIVE.silver_sessions"    
    return spark.sql(f"""
      select max(session_end_date) as session_end_date,
             date_add(max(session_end_date), 0-{yesterday_days}) as yesterday,
             date_add(max(session_end_date), 0-{yesterday_days}-{lookback_days}) as start_at
        from {source_table}
    """)

# COMMAND ----------

# This table is to demonstrate how you could do the stream AND periodic aggregation 
# in one pipeline (if stream_and_aggregate = true) or in batch mode (if only_aggregate is true)
if stream_and_aggregate or only_aggregate:
  
  spark_conf = {}
  if stream_and_aggregate:
    spark_conf = {"pipelines.trigger.interval" : stream_and_aggregate_refresh_interval}
  
  @dlt.table(
    comment = f"Aggregate profile counts per {col_media_id} over the last {lookback_days} days",
    table_properties={
      "quality": "gold",
      "delta.enableChangeDataFeed": "true",
    },
    spark_conf = spark_conf
  )
  def gold_media_profile_counts():
    source_table = f"{target}.silver_sessions" if only_aggregate else "LIVE.silver_sessions"
    spark.table(source_table).createOrReplaceTempView("silver_sessions")

    df = spark.sql(f"""

      with exploded as (
        select explode(a.media_id_set) as {col_media_id},
               a.session_end_date,
               a.{col_profile_id},
               b.start_at,
               b.yesterday
          from silver_sessions a
         inner join LIVE.max_session_end b
         where a.session_end_date between b.start_at and b.yesterday
      )
      select e.session_end_date, 
             e.{col_media_id},
             m.{col_media_name},
             count(distinct e.{col_profile_id}) unique_profiles, 
             e.start_at as start_date, 
             e.yesterday as end_date,
             {lookback_days} as lookback_days,
             {yesterday_days} as yesterday_days
        from exploded e
        join LIVE.static_media_names m on e.media_id = m.media_id
       group by
             e.session_end_date, 
             e.{col_media_id}, 
             m.{col_media_name},
             start_at,
             yesterday       
     """)

    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consider using the Feature Store to store the final dataset
# MAGIC 
# MAGIC #### Why use Databricks Feature Store?
# MAGIC Databricks Feature Store is fully integrated with other components of Databricks.
# MAGIC * Discoverability. The Feature Store UI, accessible from the Databricks workspace, lets you browse and search for existing features.
# MAGIC * Lineage. When you create a feature table with Feature Store, the data sources used to create the feature table are saved and accessible. For each feature in a feature table, you can also access the models, notebooks, jobs, and endpoints that use the feature.
# MAGIC * Integration with model scoring and serving. When you use features from Feature Store to train a model, the model is packaged with feature metadata. When you use the model for batch scoring or online inference, it automatically retrieves features from Feature Store. The caller does not need to know about them or include logic to look up or join features to score new data. This makes model deployment and updates much easier.
# MAGIC * Point-in-time lookups. Feature Store supports time series and event-based use cases that require point-in-time correctness.
# MAGIC 
# MAGIC Learn more here: https://docs.databricks.com/machine-learning/feature-store/index.html
