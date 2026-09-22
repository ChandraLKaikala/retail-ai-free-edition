# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 · End-to-End Runtime Validation — Free Edition E2E v4.0
# MAGIC Run after `00`–`06`. It verifies object existence, referential/financial consistency, data-quality execution, ML coverage, agent evaluation, AvailableNow streaming output, and the governed Unity Catalog volume. If the DAB Lakeflow pipeline has also run, its three managed realtime outputs are validated as well.

# COMMAND ----------

# DBTITLE 1,Validation Setup
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
from datetime import datetime, timezone
from pyspark.sql import Row
import uuid
spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN_ID=str(uuid.uuid4())[:8]; NOW=datetime.now(timezone.utc).replace(tzinfo=None); checks=[]
def add(name,category,status,value,expected,detail=""):
    checks.append(Row(check_name=name,category=category,status=status,actual_value=str(value),expected_value=str(expected),detail=str(detail)[:1000],run_id=RUN_ID,evaluated_at=NOW))
def scalar(sql): return spark.sql(sql).first()[0]

# COMMAND ----------

expected = [
'retail_bronze.customers','retail_bronze.orders','retail_bronze.order_items','retail_silver.fact_orders',
'retail_gold.customer_360','retail_gold.product_kpis','retail_quality.quality_results',
'retail_ml.churn_predictions','retail_genai.document_chunks','retail_genai.evaluation_results']
for obj in expected:
    ok=spark.catalog.tableExists(f"{CAT}.{obj}"); add(f"object:{obj}","objects","PASS" if ok else "FAIL",ok,True)
queries=[
("orders_customer_fk",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o LEFT ANTI JOIN {CAT}.retail_bronze.customers c ON o.customer_id=c.customer_id",0),
("items_order_fk",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.order_items i LEFT ANTI JOIN {CAT}.retail_bronze.orders o ON i.order_id=o.order_id",0),
("returns_product_order_fk",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.returns r LEFT ANTI JOIN {CAT}.retail_bronze.order_items i ON r.order_id=i.order_id AND r.product_id=i.product_id",0),
("cancelled_shipments",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.shipments s JOIN {CAT}.retail_bronze.orders o ON s.order_id=o.order_id WHERE o.status='Cancelled'",0),
("future_orders",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders WHERE to_date(order_date)>current_date()",0),
("order_gross_reconciliation",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o JOIN (SELECT order_id,ROUND(SUM(line_total),2) x FROM {CAT}.retail_bronze.order_items GROUP BY order_id) i ON o.order_id=i.order_id WHERE ABS(o.order_total-i.x)>0.01",0),
("returned_orders_have_approved_return",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o WHERE o.status='Returned' AND NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.returns r WHERE r.order_id=o.order_id AND r.status='Approved')",0),
("noncancelled_orders_have_completed_payment",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o WHERE o.status<>'Cancelled' AND NOT EXISTS (SELECT 1 FROM {CAT}.retail_bronze.payments p WHERE p.order_id=o.order_id AND p.status='Completed')",0),
("cancelled_orders_have_no_completed_payment",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders o WHERE o.status='Cancelled' AND EXISTS (SELECT 1 FROM {CAT}.retail_bronze.payments p WHERE p.order_id=o.order_id AND p.status='Completed')",0),
("negative_inventory_stock",f"SELECT COUNT(*) FROM (SELECT product_id,warehouse_id,SUM(quantity) stock FROM {CAT}.retail_bronze.inventory_events GROUP BY product_id,warehouse_id) x WHERE stock<0",0),
("shipment_delivery_temporal",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.shipments WHERE actual_delivery IS NOT NULL AND (to_date(actual_delivery)<to_date(ship_date) OR to_date(actual_delivery)>current_date())",0),
("return_date_temporal",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.returns r JOIN {CAT}.retail_bronze.orders o ON r.order_id=o.order_id WHERE to_date(r.return_date)<=to_date(o.order_date) OR to_date(r.return_date)>current_date()",0),
("dq_rule_errors",f"SELECT COUNT(*) FROM {CAT}.retail_quality.quality_results WHERE status='ERROR'",0),
("bronze_silver_order_reconciliation",f"SELECT COUNT(*) FROM {CAT}.retail_bronze.orders b LEFT ANTI JOIN {CAT}.retail_silver.fact_orders s ON b.order_id=s.order_id",0),
("silver_gold_customer_reconciliation",f"SELECT COUNT(*) FROM {CAT}.retail_silver.dim_customers s LEFT ANTI JOIN {CAT}.retail_gold.customer_360 g ON s.customer_id=g.customer_id",0),
("null_churn_predictions",f"SELECT COUNT(*) FROM {CAT}.retail_ml.churn_predictions WHERE churn_probability IS NULL",0),
("forecast_negative_units",f"SELECT COUNT(*) FROM {CAT}.retail_ml.demand_forecast WHERE predicted_units < 0",0)]
for name,sql,exp in queries:
    try:
        v=scalar(sql); add(name,"core","PASS" if v==exp else "FAIL",v,exp)
    except Exception as e: add(name,"core","FAIL","ERROR",exp,str(e))

# COMMAND ----------

# DBTITLE 1,Validation Results with History & Exceptions
try:
    customers=scalar(f"SELECT COUNT(*) FROM {CAT}.retail_silver.dim_customers"); scored=scalar(f"SELECT COUNT(*) FROM {CAT}.retail_ml.churn_predictions")
    add("ml_customer_coverage","ml","PASS" if customers==scored else "FAIL",scored,customers)
except Exception as e: add("ml_customer_coverage","ml","FAIL","ERROR","customer count",str(e))
try:
    wrong=scalar(f"SELECT COUNT(*) FROM {CAT}.retail_genai.evaluation_results WHERE tool_correct=false")
    add("agent_routing_eval","genai","PASS" if wrong==0 else "FAIL",wrong,0)
except Exception as e: add("agent_routing_eval","genai","FAIL","ERROR",0,str(e))
try:
    n=scalar(f"SELECT COUNT(*) FROM {CAT}.retail_gold.live_sales_minute")
    add("realtime_rows","streaming","PASS" if n>0 else "SKIP",n,">0","Start 06_realtime_streaming" if n==0 else "")
except Exception: add("realtime_rows","streaming","SKIP",0,">0","Streaming tables not initialized")
try:
    vols = [r.asDict() for r in spark.sql(f"SHOW VOLUMES IN {CAT}.retail_monitoring").collect()]
    has_vol = any((r.get("volume_name") or r.get("name")) == "retail_ai_files" for r in vols)
    add("uc_volume_retail_ai_files","free_edition","PASS" if has_vol else "FAIL",has_vol,True)
except Exception as e:
    add("uc_volume_retail_ai_files","free_edition","FAIL","ERROR",True,str(e))
try:
    rt = scalar(f"SELECT COUNT(*) FROM {CAT}.retail_bronze.realtime_order_events")
    ok_metrics = scalar(f"SELECT COUNT(*) FROM {CAT}.retail_monitoring.streaming_metrics WHERE stream_name='retail_orders_available_now' AND status='SUCCEEDED'")
    add("available_now_realtime_rows","streaming","PASS" if rt>0 else "FAIL",rt,">0")
    add("available_now_successful_microbatches","streaming","PASS" if ok_metrics>0 else "FAIL",ok_metrics,">0")
except Exception as e:
    add("available_now_streaming","streaming","FAIL","ERROR",">0",str(e))
for obj in ["orders_bronze_stream","orders_silver_stream","live_sales_minute_pipeline"]:
    fq=f"{CAT}.retail_realtime.{obj}"
    try:
        if spark.catalog.tableExists(fq):
            n=scalar(f"SELECT COUNT(*) FROM {fq}")
            add(f"lakeflow:{obj}","lakeflow","PASS" if n>0 else "FAIL",n,">0")
        else:
            add(f"lakeflow:{obj}","lakeflow","SKIP",False,"table exists","Run the DAB Lakeflow pipeline to validate this optional managed path.")
    except Exception as e:
        add(f"lakeflow:{obj}","lakeflow","FAIL","ERROR",">0",str(e))

res=spark.createDataFrame(checks)

# Write to latest and history tables
spark.sql(f"CREATE TABLE IF NOT EXISTS {CAT}.retail_monitoring.validation_results_latest (check_name STRING, category STRING, status STRING, actual_value STRING, expected_value STRING, detail STRING, run_id STRING, evaluated_at TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
spark.sql(f"CREATE TABLE IF NOT EXISTS {CAT}.retail_monitoring.validation_results_history (check_name STRING, category STRING, status STRING, actual_value STRING, expected_value STRING, detail STRING, run_id STRING, evaluated_at TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.logRetentionDuration' = 'interval 30 days')")

res.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CAT}.retail_monitoring.validation_results_latest")
res.write.format("delta").mode("append").saveAsTable(f"{CAT}.retail_monitoring.validation_results_history")

spark.sql(f"COMMENT ON TABLE {CAT}.retail_monitoring.validation_results_latest IS 'Latest E2E runtime validation results: object existence, referential integrity, financial consistency, ML coverage, and streaming checks'")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_monitoring.validation_results_history IS 'Historical runtime validation results for trend analysis across multiple runs'")
counts={r['status']:r['count'] for r in res.groupBy('status').count().collect()}
print(f"PASS={counts.get('PASS',0)} | FAIL={counts.get('FAIL',0)} | SKIP={counts.get('SKIP',0)}")
validation_status = 'FAILED' if counts.get('FAIL',0)>0 else 'SUCCEEDED'
validation_finished = datetime.now(timezone.utc).replace(tzinfo=None)
_val_duration = round((validation_finished - NOW).total_seconds(), 2)
run_df = spark.createDataFrame([(RUN_ID,'07_runtime_validation','validation',validation_status,int(len(checks)),NOW,validation_finished,_val_duration,PROJECT_VERSION,'free-edition',
                                 '' if validation_status=='SUCCEEDED' else 'One or more runtime validation checks failed')],
    ['run_id','notebook','layer','status','rows_written','start_time','end_time','duration_seconds','project_version','environment','error_message'])
run_df.write.format('delta').mode('append').saveAsTable(f"{CAT}.retail_monitoring.pipeline_runs")
display(res.orderBy('status','category','check_name'))
# CRITICAL: Raise exception for critical failures (core referential integrity, FK violations, reconciliation)
critical_categories = ['core', 'objects']
critical_failures = [r for r in checks if r.status == 'FAIL' and r.category in critical_categories]

if critical_failures:
    print("\n" + "="*60)
    print("❌ CRITICAL VALIDATION FAILURES - PIPELINE BLOCKED")
    print("="*60)
    for cf in critical_failures[:10]:  # Show first 10
        print(f"  • {cf.check_name}: expected {cf.expected_value}, got {cf.actual_value}")
    if len(critical_failures) > 10:
        print(f"  • ... and {len(critical_failures)-10} more critical failures")
    raise ValueError(f"Pipeline halted: {len(critical_failures)} critical validation failure(s) detected. Fix data integrity issues before proceeding.")

if counts.get('FAIL',0)>0: print("⚠ Runtime validation found non-critical failures; inspect before presenting.")
else: print("✅ All executed runtime checks passed.")
