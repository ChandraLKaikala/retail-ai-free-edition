# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Notebook Walkthrough
# MAGIC %md
# MAGIC # Notebook Walkthrough — Corrected Implementation
# MAGIC 
# MAGIC ## Run order
# MAGIC **Batch validation path:** `00_setup` → `01_bronze_ingestion` → `02_silver_gold_pipeline` → `03_data_quality_checks` → `04_ml_training` → `05_genai_agent` → `07_runtime_validation`.
# MAGIC 
# MAGIC **Live demo path:** run `06_realtime_streaming` separately, then refresh `RESULTS_DASHBOARD` or use `AGENT_CHAT`.
# MAGIC 
# MAGIC ## Notebook-by-notebook
# MAGIC **00_setup** creates nine schemas (including `retail_realtime` for streaming), 36 metadata-driven DQ rules, pipeline-run monitoring, and streaming metrics. **Tier-1:** Sets `spark.sql.session.timeZone = UTC` for standardized timestamps. Monitoring schema upgraded with `duration_seconds`, `project_version`, and `environment` fields.
# MAGIC 
# MAGIC **01_bronze_ingestion** generates 326,308 deterministic baseline rows across 15 Bronze tables. Dates are current-run anchored. Orders reconcile to line items; verified reviews map to real purchased products; inventory signs follow event semantics; returns are limited to completed/returned historical orders.
# MAGIC 
# MAGIC **02_silver_gold_pipeline** creates typed facts/dimensions and Gold marts. Recognized revenue is gross sales minus discount and approved refunds, capped at zero; cancelled orders recognize zero revenue. Product sales, refunds, and reviews are aggregated independently before joining, preventing many-to-many inflation. **Tier-2:** Table renamed `fact_inventory_snapshot` to `fact_inventory_daily_movement`. Column `predicted_3yr_clv` to `baseline_3yr_clv`. Column `churn_risk` to `recency_risk`. Explicit metric-definition comments and governance comments added to all Gold tables.
# MAGIC 
# MAGIC **03_data_quality_checks** executes all enabled SQL rules from metadata, writes latest results plus append-only history, and creates executive DQ KPIs. **Tier-1:** Pipeline raises exceptions on critical/error severity failures. Min row-count check flags empty datasets. `quality_failure_samples` table captures failing records for root-cause analysis.
# MAGIC 
# MAGIC **04_ml_training** trains demonstration churn, segmentation, and anomaly models. Churn, K-Means, and Isolation Forest artifacts include preprocessing in sklearn Pipelines and are tracked in MLflow. Demand output is explicitly a 90-day calendar-day average baseline, not a validated forecasting model. Because the data and churn label are synthetic, model metrics are demonstration metrics only. **Tier-1:** UTC timezone set; Arrow optimization enabled; monitoring schema upgraded; models registered in Unity Catalog Model Registry.
# MAGIC 
# MAGIC **05_genai_agent** builds a governed lexical knowledge index and deterministic tool router. SQL is read-only; retrieval avoids raw SQL interpolation; interactions and evaluation are logged. **Tier-1:** UTC timezone set; monitoring schema upgraded; pipeline status query includes duration; governance comments on all GenAI tables.
# MAGIC 
# MAGIC **06_realtime_streaming** uses Structured Streaming with a Unity Catalog landing-file source for the demo, checkpointed `foreachBatch`, retry-safe Delta MERGEs, minute-level Gold KPIs, and idempotent streaming telemetry. Replace the Unity Catalog landing-file source with Kafka/Event Hubs/Kinesis/Auto Loader for production. **Tier-1:** UTC timezone set; `Trigger.AvailableNow()` for serverless/Free Edition; pipeline_runs monitoring added with full schema; `event_lag_seconds` for freshness tracking.
# MAGIC 
# MAGIC **RESULTS_DASHBOARD** queries the actual generated tables, including live sales and streaming health. It does not hard-code table counts or claim universal accuracy. **Tier-2:** References updated to use `recency_risk` and `baseline_3yr_clv` columns.
# MAGIC 
# MAGIC ## Important limitations
# MAGIC This archive is a production-style demonstration, not a claim of full production readiness. Runtime compatibility, permissions, checkpoint storage, MLflow experiment access, and stream behavior must be verified on the target Databricks workspace. Real production data also requires contracts, CI/CD, environment separation, secrets, alerting, retention policies, and operational SLOs.
# MAGIC 
# MAGIC ## Tier-1 and Tier-2 Industry-Readiness Improvements Applied
# MAGIC 
# MAGIC - **UTC Timezone**: `spark.sql.session.timeZone = UTC` set in every notebook (00-07, RESULTS_DASHBOARD, AGENT_CHAT)
# MAGIC - **Monitoring Schema**: `duration_seconds`, `project_version`, `environment` added to `pipeline_runs` table and all inserts
# MAGIC - **Table Rename**: `fact_inventory_snapshot` to `fact_inventory_daily_movement` for semantic accuracy
# MAGIC - **Column Rename**: `predicted_3yr_clv` to `baseline_3yr_clv` (deterministic baseline, not prediction)
# MAGIC - **Column Rename**: `churn_risk` to `recency_risk` (measures recency, not model-predicted churn)
# MAGIC - **DQ Blocking**: Critical and error severity failures raise exceptions, halting pipeline
# MAGIC - **DQ Failure Samples**: `quality_failure_samples` table captures up to 10 failing records per rule
# MAGIC - **Min Row-Count**: Empty datasets flagged with minimum threshold check
# MAGIC - **Metric Definitions**: Explicit comments for net revenue, AOV, baseline CLV, recency risk, refund mechanics
# MAGIC - **Governance Comments**: `COMMENT ON TABLE` on all Silver/Gold/ML/GenAI/monitoring tables
# MAGIC - **Free Edition Constraints**: No GPU/deep learning; serverless `Trigger.AvailableNow()` only
