# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Step Summary
# MAGIC %md
# MAGIC # Retail AI Platform — Corrected Step Summary
# MAGIC 
# MAGIC ## What is actually implemented
# MAGIC A Databricks retail lakehouse demonstration with Bronze/Silver/Gold, 36 metadata-driven quality rules, MLflow-tracked sklearn models, a grounded deterministic agent, batch monitoring, and near-real-time Structured Streaming.
# MAGIC 
# MAGIC ## Data foundation
# MAGIC - 15 baseline Bronze tables
# MAGIC - 326,308 deterministic synthetic rows before streaming
# MAGIC - 10,000 customers, 30,000 orders, 75,000 order items, 1,000 products
# MAGIC - Current-date anchoring so recent dashboards are populated
# MAGIC - Referential consistency across orders/items/reviews/returns
# MAGIC - Signed inventory movements with realistic semantics
# MAGIC 
# MAGIC ## Financial correctness
# MAGIC - Order header totals reconcile to line-item gross values.
# MAGIC - Cancelled orders recognize zero revenue.
# MAGIC - Approved return refunds reduce recognized order revenue and are capped so revenue cannot become negative.
# MAGIC - Product KPIs separately aggregate sales, refunds, and reviews, eliminating many-to-many revenue inflation.
# MAGIC 
# MAGIC ## ML correctness
# MAGIC - Churn model logs scaler + Random Forest as one Pipeline.
# MAGIC - Segmentation logs scaler + K-Means as one Pipeline.
# MAGIC - Anomaly detection logs scaler + Isolation Forest as one Pipeline.
# MAGIC - K-Means business labels are derived from observed profiles instead of numeric cluster IDs.
# MAGIC - Demand output is a deterministic 90-day baseline; it is not presented as a trained forecasting model.
# MAGIC - All predictive metrics are demonstration-only because the source data and labels are synthetic.
# MAGIC 
# MAGIC ## Real-time path
# MAGIC Rate source → Bronze event MERGE → Silver order MERGE → affected minute/channel Gold recomputation → monitoring MERGE.
# MAGIC 
# MAGIC The stream is near-real-time micro-batch processing. It is retry-aware and uses a Unity Catalog volume checkpoint. Fresh demo runs generate IDs from timestamp + source offset so they do not collide with old target rows.
# MAGIC 
# MAGIC ## Verification status
# MAGIC The archive can be statically checked for JSON integrity, Python syntax, internal table references, and packaging integrity. Spark/Delta/MLflow/Structured Streaming execution still requires the target Databricks runtime.
# MAGIC 
# MAGIC ## Tier-1 and Tier-2 industry-readiness improvements applied
# MAGIC 
# MAGIC ### Standardization
# MAGIC - **UTC Timezone**: `spark.sql.session.timeZone = UTC` set in every notebook for consistent timestamp handling across all layers.
# MAGIC - **Monitoring Schema**: `pipeline_runs` table upgraded with `duration_seconds`, `project_version`, and `environment` columns; all notebook inserts updated to populate these fields.
# MAGIC 
# MAGIC ### Semantic Accuracy
# MAGIC - **Table Rename**: `fact_inventory_snapshot` renamed to `fact_inventory_daily_movement` to reflect that the table captures daily inventory movements, not point-in-time snapshots.
# MAGIC - **Column Rename**: `predicted_3yr_clv` renamed to `baseline_3yr_clv` in `customer_360` to clarify that the value is a deterministic baseline, not a model prediction.
# MAGIC - **Column Rename**: `churn_risk` renamed to `recency_risk` in `customer_360` to accurately reflect that the metric measures recency of last order, not model-predicted churn probability.
# MAGIC - **Metric Definitions**: Explicit SQL comments added for net revenue, average order value, baseline CLV, recency risk, and refund mechanics in the Silver/Gold pipeline.
# MAGIC 
# MAGIC ### Data Quality Hardening
# MAGIC - **Blocking Logic**: Pipeline now raises exceptions on critical and error severity DQ failures, halting execution until data issues are resolved.
# MAGIC - **Min Row-Count Check**: Empty datasets are flagged with a minimum row threshold, preventing silent processing of empty tables.
# MAGIC - **Failure Samples**: New `quality_failure_samples` Delta table captures up to 10 failing record examples per rule for root-cause analysis.
# MAGIC - **Runtime Validation**: 07_runtime_validation also raises exceptions on critical validation failures (core referential integrity, FK violations).
