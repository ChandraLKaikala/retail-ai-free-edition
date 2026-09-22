# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 · Free Edition Near-Real-Time Streaming — AvailableNow
# MAGIC ### Unity Catalog landing files → Structured Streaming → Bronze → Silver → Gold → monitoring
# MAGIC 
# MAGIC This notebook is designed for **Databricks Free Edition/serverless compute**. It does **not** use `processingTime` or an unbounded notebook stream. Each run:
# MAGIC 
# MAGIC 1. Generates a finite batch of realistic order events into a Unity Catalog Volume.
# MAGIC 2. Reads only files not already processed by the persistent streaming checkpoint.
# MAGIC 3. Uses `Trigger.AvailableNow()` to process all currently available events and exit.
# MAGIC 4. Uses retry-safe Delta `MERGE` operations for Bronze/Silver and recomputes affected Gold minute/channel keys.
# MAGIC 5. Records streaming health metrics.
# MAGIC 
# MAGIC Re-run the notebook to simulate new incoming events. For an always-on managed stream, deploy `06B_lakeflow_pipeline_source` through the companion DAB as a serverless Lakeflow pipeline.

# COMMAND ----------

# DBTITLE 1,Streaming Setup
import re
try:
    _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
except Exception:
    _default_catalog = "workspace"
dbutils.widgets.text("catalog", _default_catalog, "Target catalog")
CAT = dbutils.widgets.get("catalog").strip() or _default_catalog
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CAT):
    raise ValueError("Catalog must contain only letters, numbers, and underscores and cannot start with a number.")
PROJECT_VERSION = "4.0-free-edition-e2e-2026-09-21"

from pyspark.sql import functions as F, types as T
from datetime import datetime, timezone
import time, uuid

spark.conf.set("spark.sql.session.timeZone", "UTC")
_stream_start = datetime.now(timezone.utc).replace(tzinfo=None)
STREAM_NAME = "retail_orders_available_now"
FILE_VOLUME = "retail_ai_files"
VOLUME_ROOT = f"/Volumes/{CAT}/retail_monitoring/{FILE_VOLUME}"
INPUT_PATH = f"{VOLUME_ROOT}/incoming_orders"
CHECKPOINT = f"{VOLUME_ROOT}/checkpoints/orders_available_now_v4"
dbutils.widgets.text("events_per_run", "500", "New events this run")
EVENTS_PER_RUN = max(10, min(5000, int(dbutils.widgets.get("events_per_run"))))
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.retail_monitoring.{FILE_VOLUME} COMMENT 'Retail AI Free Edition landing files and streaming checkpoints'")
except Exception as e:
    raise RuntimeError(f"A writable Unity Catalog volume is required. Run 00_setup or create {CAT}.retail_monitoring.{FILE_VOLUME}. Details: {str(e)[:240]}")
RUN_TOKEN = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
print(f"Catalog={CAT} | mode=AvailableNow | events_this_run={EVENTS_PER_RUN}")
print(f"Input={INPUT_PATH}")
print(f"Checkpoint={CHECKPOINT}")

# COMMAND ----------

# DBTITLE 1,Streaming Target Tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.retail_bronze.realtime_order_events (
  event_id STRING, order_id STRING, customer_id STRING, store_id STRING,
  event_time TIMESTAMP, channel STRING, order_total DOUBLE, discount_amount DOUBLE,
  tax_amount DOUBLE, status STRING, source STRING, _microbatch_id LONG, _ingestion_ts TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_bronze.realtime_order_events IS 'Bronze landing for realtime order events ingested via AvailableNow streaming from UC Volume'")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.retail_silver.fact_orders_realtime (
  order_id STRING, customer_id STRING, store_id STRING, event_time TIMESTAMP,
  channel STRING, order_total DOUBLE, discount_amount DOUBLE, tax_amount DOUBLE,
  net_revenue DOUBLE, order_status STRING, source STRING, _microbatch_id LONG, _processed_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_silver.fact_orders_realtime IS 'Silver typed realtime order facts with computed net_revenue, upserted via MERGE'")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.retail_gold.live_sales_minute (
  event_minute TIMESTAMP, channel STRING, order_count LONG, unique_customers LONG,
  gross_sales DOUBLE, discounts DOUBLE, revenue DOUBLE, avg_order_value DOUBLE,
  last_updated TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_gold.live_sales_minute IS 'Gold minute-level realtime sales KPIs by channel, upserted via MERGE on (event_minute, channel)'")
print("Real-time target tables ready.")

# COMMAND ----------

base = spark.range(EVENTS_PER_RUN).withColumnRenamed("id", "seq")
landed = base.select(
    F.concat(F.lit(f"RTEVT_{RUN_TOKEN}_"), F.lpad(F.col("seq").cast("string"), 6, "0")).alias("event_id"),
    F.concat(F.lit(f"RTORD_{RUN_TOKEN}_"), F.lpad(F.col("seq").cast("string"), 6, "0")).alias("order_id"),
    F.concat(F.lit("C"), F.lpad(((F.col("seq") % 10000)+1).cast("string"), 7, "0")).alias("customer_id"),
    F.concat(F.lit("STR"), F.lpad(((F.col("seq") % 50)+1).cast("string"), 4, "0")).alias("store_id"),
    (F.current_timestamp() - F.expr("INTERVAL 1 SECOND") * (F.col("seq") % 60)).alias("event_time"),
    F.when((F.col("seq") % 3)==0,"web").when((F.col("seq") % 3)==1,"mobile").otherwise("store").alias("channel"),
    F.round(25 + (F.col("seq") % 5000) * 0.37, 2).cast("double").alias("order_total"),
    F.round(((F.col("seq") % 10) / 100.0) * (25 + (F.col("seq") % 5000) * 0.37), 2).cast("double").alias("discount_amount"),
    F.round(0.08 * ((25 + (F.col("seq") % 5000) * 0.37) - (((F.col("seq") % 10) / 100.0) * (25 + (F.col("seq") % 5000) * 0.37))), 2).cast("double").alias("tax_amount"),
    F.lit("Completed").alias("status"), F.lit("free_edition_file_demo").alias("source")
)
landed.write.mode("append").json(INPUT_PATH)
print(f"✅ Landed {EVENTS_PER_RUN} new source events for run {RUN_TOKEN}")

# COMMAND ----------

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False), T.StructField("order_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), True), T.StructField("store_id", T.StringType(), True),
    T.StructField("event_time", T.TimestampType(), False), T.StructField("channel", T.StringType(), False),
    T.StructField("order_total", T.DoubleType(), False), T.StructField("discount_amount", T.DoubleType(), False),
    T.StructField("tax_amount", T.DoubleType(), False), T.StructField("status", T.StringType(), False), T.StructField("source", T.StringType(), False),
])
stream_events = (spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 100).json(INPUT_PATH))
print("Structured Streaming file source defined with an explicit schema.")

# COMMAND ----------

# DBTITLE 1,Microbatch Processor (Optimized)
def process_microbatch(batch_df, batch_id):
    started = time.perf_counter(); started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    input_rows = bronze_rows = silver_rows = 0
    try:
        staged = (batch_df.dropDuplicates(["event_id"]).withColumn("_microbatch_id", F.lit(int(batch_id))).withColumn("_ingestion_ts", F.current_timestamp()))
        batch_stats = staged.agg(F.count("*").alias("cnt"), F.max("event_time").alias("max_event")).first()
        input_rows = batch_stats["cnt"]; max_event = batch_stats["max_event"]
        if input_rows == 0: return
        bronze_rows = input_rows
        staged.createOrReplaceTempView("_rt_bronze_batch")
        spark.sql(f"""
        INSERT INTO {CAT}.retail_bronze.realtime_order_events
        SELECT * FROM _rt_bronze_batch s
        WHERE NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.realtime_order_events t WHERE t.event_id=s.event_id)
        """)
        silver_batch = (staged.select("order_id","customer_id","store_id","event_time","channel","order_total","discount_amount","tax_amount",
            F.round(F.col("order_total")-F.col("discount_amount"),2).alias("net_revenue"),F.col("status").alias("order_status"),"source","_microbatch_id")
            .withColumn("_processed_at",F.current_timestamp()))
        silver_rows = input_rows; silver_batch.createOrReplaceTempView("_rt_silver_batch")
        spark.sql(f"""
        MERGE INTO {CAT}.retail_silver.fact_orders_realtime t USING _rt_silver_batch s ON t.order_id=s.order_id
        WHEN MATCHED AND s.event_time >= t.event_time THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
        keys = silver_batch.select(F.date_trunc("minute","event_time").alias("event_minute"),"channel").distinct(); keys.createOrReplaceTempView("_rt_keys")
        spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW _rt_gold_batch AS
        SELECT date_trunc('minute',f.event_time) AS event_minute, f.channel,
               COUNT(*) AS order_count, COUNT(DISTINCT f.customer_id) AS unique_customers,
               ROUND(SUM(f.order_total),2) AS gross_sales, ROUND(SUM(f.discount_amount),2) AS discounts,
               ROUND(SUM(f.net_revenue),2) AS revenue, ROUND(AVG(f.net_revenue),2) AS avg_order_value,
               current_timestamp() AS last_updated
        FROM {CAT}.retail_silver.fact_orders_realtime f JOIN _rt_keys k
          ON date_trunc('minute',f.event_time)=k.event_minute AND f.channel=k.channel
        GROUP BY date_trunc('minute',f.event_time), f.channel
        """)
        spark.sql(f"""
        MERGE INTO {CAT}.retail_gold.live_sales_minute t USING _rt_gold_batch s
        ON t.event_minute=s.event_minute AND t.channel=s.channel
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)
        finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        lag = max(0.0,(finished_at-max_event.replace(tzinfo=None)).total_seconds()) if max_event else 0.0
        ms = int((time.perf_counter()-started)*1000)
        metrics = [(STREAM_NAME,int(batch_id),int(input_rows),int(bronze_rows),int(silver_rows),started_at,finished_at,ms,float(lag),"SUCCEEDED","")]
        mdf = spark.createDataFrame(metrics,["stream_name","microbatch_id","input_rows","bronze_rows","silver_rows","batch_started_at","batch_finished_at","processing_ms","event_lag_seconds","status","error_message"])
        mdf.createOrReplaceTempView("_rt_metrics_batch")
        spark.sql(f"""
        MERGE INTO {CAT}.retail_monitoring.streaming_metrics t USING _rt_metrics_batch s
        ON t.stream_name=s.stream_name AND t.microbatch_id=s.microbatch_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"microbatch={batch_id} rows={input_rows} processing_ms={ms} lag_s={lag:.2f}")
    except Exception as e:
        finished_at = datetime.now(timezone.utc).replace(tzinfo=None); ms = int((time.perf_counter()-started)*1000)
        metrics = [(STREAM_NAME,int(batch_id),int(input_rows),int(bronze_rows),int(silver_rows),started_at,finished_at,ms,None,"FAILED",str(e)[:1000])]
        mdf = spark.createDataFrame(metrics,["stream_name","microbatch_id","input_rows","bronze_rows","silver_rows","batch_started_at","batch_finished_at","processing_ms","event_lag_seconds","status","error_message"])
        mdf.createOrReplaceTempView("_rt_metrics_batch")
        spark.sql(f"""
        MERGE INTO {CAT}.retail_monitoring.streaming_metrics t USING _rt_metrics_batch s
        ON t.stream_name=s.stream_name AND t.microbatch_id=s.microbatch_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """)
        raise

# COMMAND ----------

query = (stream_events.writeStream.queryName(STREAM_NAME).foreachBatch(process_microbatch).option("checkpointLocation", CHECKPOINT).trigger(availableNow=True).start())
query.awaitTermination()
print(f"✅ AvailableNow streaming run completed successfully: {query.name}")
print("Re-run this notebook later to land and process the next incremental event batch.")

# COMMAND ----------

# DBTITLE 1,Streaming Display & Monitoring
display(spark.sql(f"""SELECT event_minute, channel, order_count, unique_customers, revenue, avg_order_value, last_updated FROM {CAT}.retail_gold.live_sales_minute ORDER BY event_minute DESC, revenue DESC LIMIT 30"""))
display(spark.sql(f"""SELECT stream_name, microbatch_id, input_rows, processing_ms, event_lag_seconds, status, batch_finished_at FROM {CAT}.retail_monitoring.streaming_metrics WHERE stream_name='{STREAM_NAME}' ORDER BY microbatch_id DESC LIMIT 20"""))
print("Free Edition streaming pattern: finite landing batch + Trigger.AvailableNow() + persistent checkpoint.")
_stream_end = datetime.now(timezone.utc).replace(tzinfo=None); _stream_duration = round((_stream_end - _stream_start).total_seconds(), 2)
_run_df = spark.createDataFrame([(RUN_TOKEN.split('_')[0],'06_realtime_streaming','realtime','SUCCEEDED',EVENTS_PER_RUN,_stream_start,_stream_end,_stream_duration,PROJECT_VERSION,'free-edition','')],['run_id','notebook','layer','status','rows_written','start_time','end_time','duration_seconds','project_version','environment','error_message'])
_run_df.write.format('delta').mode('append').saveAsTable(f"{CAT}.retail_monitoring.pipeline_runs")
print(f"Pipeline monitoring row recorded | duration={_stream_duration}s")
