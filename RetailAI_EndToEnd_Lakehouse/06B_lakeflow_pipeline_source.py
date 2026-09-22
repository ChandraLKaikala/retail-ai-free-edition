# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06B · Lakeflow Managed Realtime Pipeline Source
# MAGIC **Do not run this notebook as a normal notebook.** It is source code for a Lakeflow pipeline and imports `pyspark.pipelines`, which is available only in pipeline context.
# MAGIC 
# MAGIC The companion DAB deploys it as a **serverless** pipeline. It reads the same Unity Catalog landing folder used by `06_realtime_streaming`, creates a streaming Bronze table with expectations, a streaming Silver table, and a materialized Gold minute-level view. The bundle keeps the pipeline triggered by default to conserve Free Edition quota; an optional continuous job is included but deployed paused.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T

CAT = spark.conf.get("retail.source_catalog", "workspace")
INPUT_PATH = f"/Volumes/{CAT}/retail_monitoring/retail_ai_files/incoming_orders"

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("order_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("store_id", T.StringType(), True),
    T.StructField("event_time", T.TimestampType(), False),
    T.StructField("channel", T.StringType(), False),
    T.StructField("order_total", T.DoubleType(), False),
    T.StructField("discount_amount", T.DoubleType(), False),
    T.StructField("tax_amount", T.DoubleType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("source", T.StringType(), False),
])

# COMMAND ----------

@dp.table(
    name="orders_bronze_stream",
    comment="Free Edition demo order events incrementally ingested from a Unity Catalog Volume",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL AND length(event_id) > 0")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND length(order_id) > 0")
@dp.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dp.expect_or_drop("valid_channel", "channel IN ('web','mobile','store')")
@dp.expect_or_drop("valid_amount", "order_total > 0 AND discount_amount >= 0 AND discount_amount <= order_total")
def orders_bronze_stream():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(EVENT_SCHEMA)
            .load(INPUT_PATH)
            .withColumn("_pipeline_ingested_at", F.current_timestamp()))

# COMMAND ----------

@dp.table(
    name="orders_silver_stream",
    comment="Typed, quality-filtered realtime order facts",
)
@dp.expect_or_drop("positive_net_revenue", "net_revenue >= 0")
def orders_silver_stream():
    return (spark.readStream.table("orders_bronze_stream")
            .select(
                "event_id","order_id","customer_id","store_id","event_time","channel",
                "order_total","discount_amount","tax_amount",
                F.round(F.col("order_total")-F.col("discount_amount"),2).alias("net_revenue"),
                F.col("status").alias("order_status"),"source","_pipeline_ingested_at"
            ))

# COMMAND ----------

@dp.materialized_view(
    name="live_sales_minute_pipeline",
    comment="Minute/channel realtime sales KPIs maintained by Lakeflow",
)
def live_sales_minute_pipeline():
    return (spark.read.table("orders_silver_stream")
            .withColumn("event_minute", F.date_trunc("minute", F.col("event_time")))
            .groupBy("event_minute","channel")
            .agg(
                F.count("order_id").alias("order_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.round(F.sum("order_total"),2).alias("gross_sales"),
                F.round(F.sum("discount_amount"),2).alias("discounts"),
                F.round(F.sum("net_revenue"),2).alias("revenue"),
                F.round(F.avg("net_revenue"),2).alias("avg_order_value"),
                F.max("_pipeline_ingested_at").alias("last_updated"),
            ))
