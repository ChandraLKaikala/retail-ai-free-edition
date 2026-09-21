# Databricks notebook source
# MAGIC %md
# MAGIC # RetailHub Platform — Results Dashboard
# MAGIC ### Batch history + near-real-time streaming views
# MAGIC
# MAGIC The optimized dashboard references tables created by this project. Run the numbered pipeline notebooks first. For Free Edition live data, run `06_realtime_streaming`; each invocation processes a new AvailableNow increment and exits. Re-run it, then refresh the live cells. The DAB also provides a managed Lakeflow pipeline path.
# MAGIC
# MAGIC Exact object counts should be queried dynamically from the selected catalog; `PLATFORM_GUIDE` includes the inventory query.

# COMMAND ----------

print(f"Dashboard context ready: {CAT}")

# COMMAND ----------

# MAGIC %md ## Executive KPI

# COMMAND ----------

display(spark.sql(f"""
SELECT
  CAST(total_customers AS INT) as total_customers,
  CAST(active_customers AS INT) as active_customers,
  CAST(order_attempts AS INT) as order_attempts,
  CAST(total_orders AS INT) as recognized_orders,
  ROUND(CAST(total_revenue AS DOUBLE),2) as total_revenue_usd,
  ROUND(CAST(avg_order_value AS DOUBLE),2) as avg_order_value_usd,
  CAST(total_returns AS INT) as total_returns,
  CAST(active_products AS INT) as active_products,
  CAST(open_tickets AS INT) as open_support_tickets
FROM {CAT}.retail_gold.executive_kpi
"""))

# COMMAND ----------

# MAGIC %md ## Revenue by Channel (Last 30 Days)

# COMMAND ----------

display(spark.sql(f"""
SELECT order_date, channel,
  total_orders, ROUND(total_revenue,2) as revenue, ROUND(avg_order_value,2) as aov
FROM {CAT}.retail_gold.daily_sales_summary
WHERE to_date(order_date) >= date_sub(current_date(), 30)
ORDER BY order_date DESC, revenue DESC
"""))

# COMMAND ----------

# MAGIC %md ## Customer 360 — Top 20 by Revenue

# COMMAND ----------

display(spark.sql(f"""
SELECT customer_id, full_name, customer_segment, country,
  total_orders, ROUND(total_revenue,2) as lifetime_revenue,
  days_since_last_order, churn_risk, clv_tier,
  ROUND(predicted_3yr_clv,2) as predicted_3yr_clv
FROM {CAT}.retail_gold.customer_360
ORDER BY total_revenue DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## ML Model Results — Churn Risk Distribution

# COMMAND ----------

display(spark.sql(f"""
SELECT
  CASE WHEN churn_probability >= 0.7 THEN 'High (>=70%)'
       WHEN churn_probability >= 0.4 THEN 'Medium (40-70%)'
       ELSE 'Low (<40%)' END as risk_band,
  COUNT(*) as customer_count,
  ROUND(AVG(churn_probability)*100,1) as avg_churn_pct
FROM {CAT}.retail_ml.churn_predictions
GROUP BY 1 ORDER BY avg_churn_pct DESC
"""))

# COMMAND ----------

# MAGIC %md ## ML — Customer Segments

# COMMAND ----------

display(spark.sql(f"""
SELECT s.segment_label,
  COUNT(*) as customers,
  ROUND(AVG(c.total_revenue),2) as avg_revenue,
  ROUND(AVG(c.total_orders),1) as avg_orders,
  ROUND(AVG(c.days_since_last_order),0) as avg_recency_days
FROM {CAT}.retail_ml.customer_segments s
JOIN {CAT}.retail_gold.customer_kpis c ON s.customer_id = c.customer_id
GROUP BY s.segment_label
ORDER BY avg_revenue DESC
"""))

# COMMAND ----------

# MAGIC %md ## Anomaly Detection — Top Flagged Customers

# COMMAND ----------

display(spark.sql(f"""
SELECT a.entity_id as customer_id, c.customer_segment, c.country,
  ROUND(a.anomaly_score,4) as anomaly_score,
  ROUND(a.churn_probability,4) as churn_probability,
  c.total_revenue, c.days_since_last_order
FROM {CAT}.retail_monitoring.anomaly_events a
JOIN {CAT}.retail_gold.customer_360 c ON a.entity_id = c.customer_id
ORDER BY a.churn_probability DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## Demand Baseline — Top 20 Products

# COMMAND ----------

display(spark.sql(f"""
SELECT d.product_id, p.product_name, p.category_name, p.price,
  d.predicted_units, ROUND(d.stability_score*100,1) as stability_score_pct,
  ROUND(d.predicted_units * p.price, 2) as baseline_revenue_estimate
FROM {CAT}.retail_ml.demand_forecast d
JOIN {CAT}.retail_silver.dim_products p ON d.product_id = p.product_id
ORDER BY predicted_units DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## Inventory Health

# COMMAND ----------

display(spark.sql(f"""
SELECT stock_status, COUNT(*) as product_warehouse_combos,
  SUM(current_stock) as total_units,
  SUM(CASE WHEN reorder_flag THEN 1 ELSE 0 END) as reorder_needed
FROM {CAT}.retail_gold.inventory_health
GROUP BY stock_status
ORDER BY CASE stock_status
  WHEN 'Out of Stock' THEN 1 WHEN 'Critical' THEN 2
  WHEN 'Low' THEN 3 WHEN 'Normal' THEN 4 ELSE 5 END
"""))

# COMMAND ----------

# MAGIC %md ## Data Quality — All Rules

# COMMAND ----------

display(spark.sql(f"""
SELECT r.rule_id, r.rule_type, r.dataset, r.field, r.severity,
       r.description,
       q.total_records, q.failed_records,
       ROUND(q.failure_pct, 4) as failure_pct,
       q.threshold as threshold_pct,
       q.status
FROM {CAT}.retail_quality.quality_results q
JOIN {CAT}.retail_quality.quality_rules r ON q.rule_id = r.rule_id
ORDER BY CASE q.severity
         WHEN 'critical' THEN 1 WHEN 'error' THEN 2
         WHEN 'warning' THEN 3 WHEN 'informational' THEN 4 END,
         r.rule_id
"""))

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
print(f"Loading RetailHub results dashboard from catalog={CAT}...")
try:
    display(spark.sql(f"""
    SELECT severity, rule_count, passed, failed, errors, pass_rate_pct
    FROM {CAT}.retail_quality.quality_summary
    ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'error' THEN 2 WHEN 'warning' THEN 3 ELSE 4 END
    """))
except Exception as e:
    print(f"Quality summary is not ready yet: {str(e)[:160]}")

# COMMAND ----------

# MAGIC %md ## Agent Evaluation — Measured Test Set

# COMMAND ----------

display(spark.sql(f"""
SELECT question_type, COUNT(*) as questions,
  SUM(CASE WHEN tool_correct THEN 1 ELSE 0 END) as correctly_routed,
  SUM(CASE WHEN response_supported THEN 1 ELSE 0 END) as supported_responses,
  ROUND(AVG(latency_ms),1) as avg_latency_ms,
  ROUND(AVG(CASE WHEN tool_correct THEN 1.0 ELSE 0.0 END)*100,1) as routing_accuracy_pct
FROM {CAT}.retail_genai.evaluation_results
GROUP BY question_type
ORDER BY question_type
"""))

# COMMAND ----------

# MAGIC %md ## Agent Interactions Log

# COMMAND ----------

display(spark.sql(f"""
SELECT interaction_id, session_id, query, agent_name, tool_used, status,
       ROUND(latency_ms,1) as latency_ms,
       SUBSTRING(response_summary,1,180) as response_preview, created_at
FROM {CAT}.retail_genai.agent_interactions
ORDER BY created_at DESC
"""))

# COMMAND ----------

# MAGIC %md ## Real-Time Sales — Latest Minutes

# COMMAND ----------

try:
    display(spark.sql(f"""
    SELECT event_minute, channel, order_count, unique_customers,
           ROUND(gross_sales,2) as gross_sales,
           ROUND(discounts,2) as discounts,
           ROUND(revenue,2) as revenue,
           ROUND(avg_order_value,2) as avg_order_value,
           last_updated
    FROM {CAT}.retail_gold.live_sales_minute
    ORDER BY event_minute DESC, revenue DESC
    LIMIT 50
    """))
except Exception:
    print("Run 06_realtime_streaming once to initialize AvailableNow live sales tables.")

# COMMAND ----------

# MAGIC %md ## Streaming Health & Micro-batch Latency

# COMMAND ----------

try:
    display(spark.sql(f"""
    SELECT stream_name, microbatch_id, input_rows, bronze_rows, silver_rows,
           processing_ms, event_lag_seconds, status, batch_finished_at,
           ROUND(input_rows / GREATEST(processing_ms/1000.0,0.001),2) as processing_rows_per_sec
    FROM {CAT}.retail_monitoring.streaming_metrics
    ORDER BY batch_finished_at DESC
    LIMIT 50
    """))
except Exception:
    print("No streaming metrics yet. Run 06_realtime_streaming once.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## How to use the project interactively
# MAGIC - **AGENT_CHAT**: ask policy, KPI, quality, anomaly, customer, and live-stream questions.
# MAGIC - **06_realtime_streaming**: land and process the next serverless-safe AvailableNow increment.
# MAGIC - **SQL Editor**: query any `workspace.retail_*` table directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Optimized Runtime Architecture
# MAGIC
# MAGIC **Implemented in this archive:**
# MAGIC - Current-date synthetic baseline so recent-period dashboards are populated.
# MAGIC - Relationally consistent orders/items/shipments/returns and signed inventory events.
# MAGIC - Correct product KPI aggregation without review-driven revenue multiplication.
# MAGIC - Reproducible ML preprocessing/model logging and deterministic demand baseline.
# MAGIC - Schema-aligned, read-only GenAI tools with measurable evaluation results.
# MAGIC - Structured Streaming demo with a Unity Catalog volume checkpoint, idempotent Delta MERGE, live minute KPIs, and micro-batch monitoring.
# MAGIC
# MAGIC **Production adapters still needed:** replace the Unity Catalog landing files source with the real event bus, define service-level objectives/alerts, configure secrets/permissions, and deploy notebooks as Databricks Jobs/Declarative Automation Bundles.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Managed Lakeflow Realtime Path (optional DAB deployment)
# MAGIC If the companion DAB pipeline has run, this shows its independently managed streaming/materialized outputs.

# COMMAND ----------

try:
    display(spark.sql(f"""
    SELECT event_minute, channel, order_count, unique_customers, revenue, avg_order_value, last_updated
    FROM {CAT}.retail_realtime.live_sales_minute_pipeline
    ORDER BY event_minute DESC, revenue DESC
    LIMIT 50
    """))
except Exception:
    print("Managed Lakeflow output not present yet. Deploy/run the companion DAB pipeline or use the AvailableNow dashboard above.")
