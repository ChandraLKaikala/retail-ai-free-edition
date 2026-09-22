# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Project Setup — Free Edition E2E v4.0
# MAGIC Creates the governed schemas, metadata-driven quality rules, monitoring tables, and a Unity Catalog managed volume used by the Free Edition streaming demo.
# MAGIC
# MAGIC **Manual notebook order:** `00_setup` → `01_bronze_ingestion` → `02_silver_gold_pipeline` → `03_data_quality_checks` → `04_ml_training` → `05_genai_agent` → `06_realtime_streaming` → `07_runtime_validation`.
# MAGIC
# MAGIC `06_realtime_streaming` is deliberately **serverless-safe**: it lands a finite event batch in a Unity Catalog Volume, consumes all newly available files with Structured Streaming `Trigger.AvailableNow()`, updates Bronze/Silver/Gold, then exits. Re-run it to simulate the next near-real-time increment.
# MAGIC
# MAGIC `06B_lakeflow_pipeline_source` is the managed Lakeflow pipeline source used by the companion Declarative Automation Bundle (DAB). Do not run that notebook directly; Lakeflow evaluates it in pipeline context.

# COMMAND ----------

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

schemas = ["retail_bronze", "retail_silver", "retail_gold", "retail_ml",
           "retail_genai", "retail_quality", "retail_monitoring", "retail_metrics", "retail_realtime"]
for s in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{s}")
    print(f"✓ Schema ready: {CAT}.{s}")

for key, value in {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
}.items():
    try:
        spark.conf.set(key, value)
    except Exception:
        pass

print(f"\n✅ Catalog: {CAT} | schemas ready: {len(schemas)} | version: {PROJECT_VERSION}")

# COMMAND ----------

# DBTITLE 1,Quality Rules Table
from pyspark.sql import Row
spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_quality.quality_rules (rule_id STRING,dataset STRING,field STRING,rule_type STRING,description STRING,severity STRING,threshold DOUBLE,enabled BOOLEAN,check_expression STRING) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors': 'true')")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_quality.quality_rules IS 'Metadata-driven data quality rules: each row defines a SQL check_expression evaluated against Bronze tables'")
rules = [
  ("QR001","customers","customer_id","uniqueness","No duplicate customer IDs","critical",0.0,True,f"SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM {CAT}.retail_bronze.customers"),
  ("QR002","customers","email","completeness","Email is populated","error",1.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.customers WHERE email IS NULL OR TRIM(email) = ''"),
  ("QR003","customers","email","validity","Email has a basic valid shape","warning",5.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.customers WHERE email NOT LIKE '%@%.%'"),
  ("QR004","customers","registration_date","validity","Registration date is not in the future","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.customers WHERE to_date(registration_date) > current_date()"),
  ("QR005","orders","customer_id","referential_integrity","Orders reference valid customers","critical",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders WHERE customer_id NOT IN (SELECT customer_id FROM {CAT}.retail_bronze.customers)"),
  ("QR006","orders","order_total","validity","Gross order total is positive","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders WHERE order_total <= 0"),
  ("QR007","orders","order_date","timeliness","Order date is not in the future","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders WHERE to_date(order_date) > current_date()"),
  ("QR008","order_items","quantity","validity","Item quantity is positive","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.order_items WHERE quantity <= 0"),
  ("QR009","order_items","unit_price","validity","Item price is positive","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.order_items WHERE unit_price <= 0"),
  ("QR010","payments","amount","validity","Payment/refund amount is positive","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.payments WHERE amount <= 0"),
  ("QR011","payments","order_id","referential_integrity","Payments reference valid orders","critical",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.payments WHERE order_id NOT IN (SELECT order_id FROM {CAT}.retail_bronze.orders)"),
  ("QR012","products","product_id","uniqueness","No duplicate product IDs","critical",0.0,True,f"SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM {CAT}.retail_bronze.products"),
  ("QR013","products","price","validity","Product price is positive","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.products WHERE price <= 0"),
  ("QR014","inventory_events","quantity","validity","Inventory movement is non-zero; signed quantities are valid","informational",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.inventory_events WHERE quantity = 0"),
  ("QR015","shipments","order_id","referential_integrity","Shipments reference valid orders","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.shipments WHERE order_id NOT IN (SELECT order_id FROM {CAT}.retail_bronze.orders)"),
  ("QR016","clickstream_events","session_id","completeness","Clickstream session is populated","warning",2.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.clickstream_events WHERE session_id IS NULL OR TRIM(session_id)=''"),
  ("QR017","support_tickets","customer_id","referential_integrity","Tickets reference valid customers","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.support_tickets WHERE customer_id NOT IN (SELECT customer_id FROM {CAT}.retail_bronze.customers)"),
  ("QR018","returns","order_id","referential_integrity","Returns reference valid orders","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.returns WHERE order_id NOT IN (SELECT order_id FROM {CAT}.retail_bronze.orders)"),
  ("QR019","customers","customer_segment","validity","Customer segment is recognized","warning",5.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.customers WHERE customer_segment NOT IN ('Basic','Standard','Premium','VIP')"),
  ("QR020","orders","status","validity","Order status is recognized","warning",2.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders WHERE status NOT IN ('Processing','Shipped','Completed','Cancelled','Returned')"),
  ("QR021","products","category_id","referential_integrity","Products reference valid categories","warning",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.products WHERE category_id NOT IN (SELECT category_id FROM {CAT}.retail_bronze.categories)"),
  ("QR022","shipments","status","validity","Shipment status is recognized","warning",2.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.shipments WHERE status NOT IN ('Processing','In Transit','Delivered','Failed')"),
  ("QR023","payments","status","validity","Payment status is recognized","warning",2.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.payments WHERE status NOT IN ('Completed','Failed','Refunded')"),
  ("QR024","customers","country","validity","Country code is two characters","warning",5.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.customers WHERE LENGTH(country) != 2"),
  ("QR025","orders","order_total","consistency","Gross order total reconciles to item lines","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o JOIN (SELECT order_id, ROUND(SUM(line_total),2) item_total FROM {CAT}.retail_bronze.order_items GROUP BY order_id) i ON o.order_id=i.order_id WHERE ABS(o.order_total-i.item_total)>0.01"),
  ("QR026","orders","discount_amount","consistency","Header discount reconciles to line discounts","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o JOIN (SELECT order_id, ROUND(SUM(line_total*discount_pct),2) discount_total FROM {CAT}.retail_bronze.order_items GROUP BY order_id) i ON o.order_id=i.order_id WHERE ABS(o.discount_amount-i.discount_total)>0.01"),
  ("QR027","orders","tax_amount","consistency","Tax equals 8% of discounted gross","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders WHERE ABS(tax_amount-ROUND((order_total-discount_amount)*0.08,2))>0.01"),
  ("QR028","returns","product_id","referential_integrity","Returned product belongs to the referenced order","critical",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.returns r WHERE NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.order_items i WHERE i.order_id=r.order_id AND i.product_id=r.product_id)"),
  ("QR029","shipments","order_id","consistency","Cancelled orders are not shipped","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.shipments s JOIN {CAT}.retail_bronze.orders o ON s.order_id=o.order_id WHERE o.status='Cancelled'"),
  ("QR030","product_reviews","verified_purchase","referential_integrity","Verified reviews match an eligible completed/returned purchase","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.product_reviews r WHERE r.verified_purchase=true AND NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.orders o JOIN {CAT}.retail_bronze.order_items i ON o.order_id=i.order_id WHERE o.customer_id=r.customer_id AND i.product_id=r.product_id AND o.status IN ('Completed','Returned'))"),
  ("QR031","orders","status","consistency","Every Returned order has an approved return","critical",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o WHERE o.status='Returned' AND NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.returns r WHERE r.order_id=o.order_id AND r.status='Approved')"),
  ("QR032","payments","status","consistency","Every non-cancelled order has a completed payment","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o WHERE o.status<>'Cancelled' AND NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.payments p WHERE p.order_id=o.order_id AND p.status='Completed')"),
  ("QR033","payments","status","consistency","Cancelled orders do not retain a completed payment","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o WHERE o.status='Cancelled' AND EXISTS (SELECT 1 FROM {CAT}.retail_bronze.payments p WHERE p.order_id=o.order_id AND p.status='Completed')"),
  ("QR034","inventory_events","quantity","consistency","Aggregate physical stock is not negative","error",0.0,True,f"SELECT COUNT(*) FROM (SELECT product_id,warehouse_id,SUM(quantity) AS stock FROM {CAT}.retail_bronze.inventory_events GROUP BY product_id,warehouse_id) x WHERE stock<0"),
  ("QR035","shipments","actual_delivery","consistency","Actual delivery is on/after ship date and not in the future","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.shipments WHERE actual_delivery IS NOT NULL AND (to_date(actual_delivery)<to_date(ship_date) OR to_date(actual_delivery)>current_date())"),
  ("QR036","returns","return_date","consistency","Return date is after the order date and not in the future","error",0.0,True,f"SELECT COUNT(*) FROM {CAT}.retail_bronze.returns r JOIN {CAT}.retail_bronze.orders o ON r.order_id=o.order_id WHERE to_date(r.return_date)<=to_date(o.order_date) OR to_date(r.return_date)>current_date()"),
]
spark.createDataFrame([Row(rule_id=r[0],dataset=r[1],field=r[2],rule_type=r[3],description=r[4],severity=r[5],threshold=r[6],enabled=r[7],check_expression=r[8]) for r in rules]) \
    .write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.retail_quality.quality_rules")
print(f"quality_rules: {len(rules)}")

# COMMAND ----------

# DBTITLE 1,Monitoring Tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.retail_monitoring.pipeline_runs (
  run_id STRING, notebook STRING, layer STRING, status STRING, rows_written LONG,
  start_time TIMESTAMP, end_time TIMESTAMP, error_message STRING
) USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors': 'true',
  'delta.deletedFileRetentionDuration': 'interval 7 days',
  'delta.logRetentionDuration': 'interval 14 days'
)
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_monitoring.pipeline_runs IS 'Pipeline execution audit log: one row per notebook run with status, row count, and timing'")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.retail_monitoring.streaming_metrics (
  stream_name STRING, microbatch_id LONG, input_rows LONG, bronze_rows LONG,
  silver_rows LONG, batch_started_at TIMESTAMP, batch_finished_at TIMESTAMP,
  processing_ms LONG, event_lag_seconds DOUBLE, status STRING, error_message STRING
) USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors': 'true',
  'delta.deletedFileRetentionDuration': 'interval 7 days',
  'delta.logRetentionDuration': 'interval 14 days'
)
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_monitoring.streaming_metrics IS 'Streaming microbatch health metrics: processing time, event lag, and row counts per batch'")
_stream_cols = {f.name for f in spark.table(f"{CAT}.retail_monitoring.streaming_metrics").schema.fields}
if "event_lag_seconds" not in _stream_cols:
    spark.sql(f"ALTER TABLE {CAT}.retail_monitoring.streaming_metrics ADD COLUMNS (event_lag_seconds DOUBLE)")

FILE_VOLUME = "retail_ai_files"
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.retail_monitoring.{FILE_VOLUME} COMMENT 'Retail AI Free Edition landing files and streaming checkpoints'")
    print(f"✓ Unity Catalog volume ready: {CAT}.retail_monitoring.{FILE_VOLUME}")
except Exception as e:
    print("⚠ Could not create the managed volume automatically.")
    print(f"  Grant CREATE VOLUME on {CAT}.retail_monitoring or create {CAT}.retail_monitoring.{FILE_VOLUME} manually.")
    print(f"  Details: {str(e)[:220]}")

print("\n" + "="*60)
print("✅ SETUP COMPLETE — FREE EDITION READY")
print("="*60)
print(f"Catalog: {CAT} | Schemas: {len(schemas)} | Quality rules: {len(rules)}")
print("Storage: Unity Catalog managed volume (no DBFS root dependency)")
print("Streaming mode: Trigger.AvailableNow() for serverless notebooks/jobs")
print("="*60)