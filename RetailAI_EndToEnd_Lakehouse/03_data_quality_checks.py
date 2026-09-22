# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 - Data Quality Checks
# MAGIC ### Metadata-Driven Quality Framework
# MAGIC 
# MAGIC **How it works:** This notebook reads all enabled rules from `retail_quality.quality_rules` and dynamically executes each rule's `check_expression` SQL against the Bronze layer. No hardcoded checks — add or modify rules in the table and re-run.
# MAGIC 
# MAGIC **Quality Rules:** **36 active rules** across all Bronze tables
# MAGIC 
# MAGIC **Rule Types:**
# MAGIC * Uniqueness (primary keys)
# MAGIC * Completeness (NOT NULL checks)
# MAGIC * Validity (format, range, domain)
# MAGIC * Referential Integrity (foreign keys)
# MAGIC * Timeliness (future date checks)
# MAGIC * Consistency (cross-field validation)
# MAGIC 
# MAGIC **Severity Levels:** Critical, Error, Warning, Informational
# MAGIC 
# MAGIC **Output:** Pass/Fail status, violation counts, failure percentages — written to `quality_results` and aggregated into `quality_summary`

# COMMAND ----------

# DBTITLE 1,DQ Engine with Blocking Failures
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
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row

spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN_ID = str(uuid.uuid4())[:8]
NOW = datetime.now(timezone.utc).replace(tzinfo=None)

RESULT_SCHEMA = """
  result_id STRING, rule_id STRING, dataset STRING, field STRING, severity STRING,
  rule_type STRING, description STRING, total_records LONG, passed_records LONG,
  failed_records LONG, failure_pct DOUBLE, threshold DOUBLE, status STRING,
  run_id STRING, evaluated_at TIMESTAMP
"""
spark.sql(f"CREATE TABLE IF NOT EXISTS {CAT}.retail_quality.quality_results ({RESULT_SCHEMA}) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
spark.sql(f"CREATE TABLE IF NOT EXISTS {CAT}.retail_quality.quality_results_history ({RESULT_SCHEMA}) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.logRetentionDuration' = 'interval 30 days')")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_quality.quality_results IS 'Latest data quality evaluation results: one row per rule per run with pass/fail status and violation counts'")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_quality.quality_results_history IS 'Historical data quality results retained for trend analysis across multiple runs'")

# Apply primary key constraint on customers.customer_id so optimizer can skip redundant grouping
try:
    spark.sql(f"ALTER TABLE {CAT}.retail_bronze.customers ADD CONSTRAINT pk_customers_id PRIMARY KEY (customer_id)")
except Exception:
    pass

rules = spark.sql(f"""
  SELECT rule_id, dataset, field, rule_type, description, severity, threshold, check_expression
  FROM {CAT}.retail_quality.quality_rules
  WHERE enabled = true ORDER BY rule_id
""").collect()

# Cache dataset counts to avoid repeated scans
dataset_counts = {}
for r in rules:
    if r.dataset not in dataset_counts:
        try:
            dataset_counts[r.dataset] = spark.sql(f"SELECT COUNT(*) FROM {CAT}.retail_bronze.{r.dataset}").first()[0]
        except Exception:
            dataset_counts[r.dataset] = 0

# Min row-count check: datasets must have data to be considered healthy
min_row_threshold = 10
for dataset, count in dataset_counts.items():
    if count < min_row_threshold:
        print(f"⚠ EMPTY DATASET: {dataset} has only {count} rows (min {min_row_threshold} required)")

results, errors, failure_samples = [], [], []
critical_failures, error_failures = [], []

for r in rules:
    try:
        total = dataset_counts.get(r.dataset, 0)
        failed = spark.sql(r.check_expression).first()[0]
        pct = round(failed * 100.0 / total, 4) if total else 0.0
        status = "PASS" if pct <= r.threshold else "FAIL"
        
        # Capture failure samples for failed rules (limit 10 examples)
        if status == "FAIL" and failed > 0:
            try:
                # Extract the failing records query from check_expression
                sample_query = r.check_expression.replace("SELECT COUNT(*)", "SELECT *").replace("WHERE", "WHERE") + " LIMIT 10"
                sample_df = spark.sql(sample_query)
                for row in sample_df.collect():
                    failure_samples.append(Row(
                        sample_id=f"{r.rule_id}_{RUN_ID}_{len(failure_samples)}",
                        rule_id=r.rule_id, dataset=r.dataset, field=r.field,
                        failing_value=str(row)[:500], run_id=RUN_ID, captured_at=NOW
                    ))
            except Exception:
                pass  # Sample capture is optional
        
    except Exception as e:
        errors.append(f"{r.rule_id}: {str(e)[:200]}")
        total, failed, pct, status = dataset_counts.get(r.dataset, 0), 0, 0.0, "ERROR"
    
    results.append(Row(
        result_id=f"{r.rule_id}_{RUN_ID}", rule_id=r.rule_id, dataset=r.dataset,
        field=r.field, severity=r.severity, rule_type=r.rule_type, description=r.description,
        total_records=int(total), passed_records=max(int(total-failed),0), failed_records=int(failed),
        failure_pct=float(pct), threshold=float(r.threshold), status=status,
        run_id=RUN_ID, evaluated_at=NOW
    ))
    
    # Track blocking failures
    if status == "FAIL" and r.severity == "critical":
        critical_failures.append(f"{r.rule_id}: {r.description} ({failed} violations, {pct}%)")
    elif status in ("FAIL", "ERROR") and r.severity == "error":
        error_failures.append(f"{r.rule_id}: {r.description} ({failed} violations, {pct}%)")

# Write results
result_df = spark.createDataFrame(results)
result_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CAT}.retail_quality.quality_results")
result_df.write.format("delta").mode("append").saveAsTable(f"{CAT}.retail_quality.quality_results_history")

# Write failure samples
if failure_samples:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CAT}.retail_quality.quality_failure_samples (
      sample_id STRING, rule_id STRING, dataset STRING, field STRING,
      failing_value STRING, run_id STRING, captured_at TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
    """)
    spark.sql(f"COMMENT ON TABLE {CAT}.retail_quality.quality_failure_samples IS 'Sample failing records (max 10 per rule) captured during DQ evaluation for root cause analysis'")
    sample_df = spark.createDataFrame(failure_samples)
    sample_df.write.format("delta").mode("append").saveAsTable(f"{CAT}.retail_quality.quality_failure_samples")
    print(f"✓ Captured {len(failure_samples)} failure samples")

passed = sum(r.status == "PASS" for r in results)
failed = sum(r.status == "FAIL" for r in results)
errored = sum(r.status == "ERROR" for r in results)
print(f"DQ Engine: {len(rules)} rules | {passed} PASS | {failed} FAIL | {errored} ERROR")
for e in errors:
    print(f"⚠ {e}")

# BLOCKING FAILURES: Critical and Error severity failures stop the pipeline
if critical_failures:
    print("\n" + "="*60)
    print("❌ CRITICAL DQ FAILURES - PIPELINE BLOCKED")
    print("="*60)
    for f in critical_failures:
        print(f"  • {f}")
    raise ValueError(f"Pipeline halted: {len(critical_failures)} critical data quality failure(s) detected. Fix data issues before proceeding.")

if error_failures:
    print("\n" + "="*60)
    print("❌ ERROR DQ FAILURES - PIPELINE BLOCKED")
    print("="*60)
    for f in error_failures:
        print(f"  • {f}")
    raise ValueError(f"Pipeline halted: {len(error_failures)} error-level data quality failure(s) detected. Fix data issues before proceeding.")

# COMMAND ----------

# DBTITLE 1,DQ Summary
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_quality.quality_summary AS
SELECT severity, COUNT(*) AS rule_count,
       SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) AS errors,
       ROUND(SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS pass_rate_pct
FROM {CAT}.retail_quality.quality_results GROUP BY severity
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_quality.quality_summary IS 'Aggregated DQ pass/fail counts and pass rate by severity level'")
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.data_quality_kpi AS
SELECT current_date() AS report_date, SUM(rule_count) AS total_rules, SUM(passed) AS total_passed,
       SUM(failed) AS total_failed, SUM(errors) AS total_errors,
       ROUND(SUM(passed)*100.0/SUM(rule_count),1) AS overall_pass_rate_pct
FROM {CAT}.retail_quality.quality_summary
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_gold.data_quality_kpi IS 'Single-row DQ KPI: total rules, passed, failed, errors, and overall pass rate'")
engine_status = 'FAILED' if errored else 'SUCCEEDED'
error_message = ' | '.join(errors)[:1000]
_dq_end = datetime.now(timezone.utc).replace(tzinfo=None)
_dq_duration = round((_dq_end - NOW).total_seconds(), 2)
run_df = spark.createDataFrame([(RUN_ID,'03_data_quality_checks','quality',engine_status,int(len(results)),NOW,_dq_end,_dq_duration,PROJECT_VERSION,'free-edition',error_message)],
    ['run_id','notebook','layer','status','rows_written','start_time','end_time','duration_seconds','project_version','environment','error_message'])
run_df.write.format('delta').mode('append').saveAsTable(f"{CAT}.retail_monitoring.pipeline_runs")
print(f"Quality complete | rules={len(results)} | pass={passed} | fail={failed} | errors={errored} | engine={engine_status} | duration={_dq_duration}s")
