# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Walkthrough — Corrected Implementation
# MAGIC
# MAGIC ## Run order
# MAGIC **Batch validation path:** `00_setup` → `01_bronze_ingestion` → `02_silver_gold_pipeline` → `03_data_quality_checks` → `04_ml_training` → `05_genai_agent` → `07_runtime_validation`.
# MAGIC
# MAGIC **Live demo path:** run `06_realtime_streaming` separately, then refresh `RESULTS_DASHBOARD` or use `AGENT_CHAT`.
# MAGIC
# MAGIC ## Notebook-by-notebook
# MAGIC **00_setup** creates eight schemas, 36 metadata-driven DQ rules, pipeline-run monitoring, and streaming metrics.
# MAGIC
# MAGIC **01_bronze_ingestion** generates 326,308 deterministic baseline rows across 15 Bronze tables. Dates are current-run anchored. Orders reconcile to line items; verified reviews map to real purchased products; inventory signs follow event semantics; returns are limited to completed/returned historical orders.
# MAGIC
# MAGIC **02_silver_gold_pipeline** creates typed facts/dimensions and Gold marts. Recognized revenue is gross sales minus discount and approved refunds, capped at zero; cancelled orders recognize zero revenue. Product sales, refunds, and reviews are aggregated independently before joining, preventing many-to-many inflation.
# MAGIC
# MAGIC **03_data_quality_checks** executes all enabled SQL rules from metadata, writes latest results plus append-only history, and creates executive DQ KPIs.
# MAGIC
# MAGIC **04_ml_training** trains demonstration churn, segmentation, and anomaly models. Churn, K-Means, and Isolation Forest artifacts include preprocessing in sklearn Pipelines and are tracked in MLflow. Demand output is explicitly a 90-day calendar-day average baseline, not a validated forecasting model. Because the data and churn label are synthetic, model metrics are demonstration metrics only.
# MAGIC
# MAGIC **05_genai_agent** builds a governed lexical knowledge index and deterministic tool router. SQL is read-only; retrieval avoids raw SQL interpolation; interactions and evaluation are logged.
# MAGIC
# MAGIC **06_realtime_streaming** uses Structured Streaming with a Unity Catalog landing-file source for the demo, checkpointed `foreachBatch`, retry-safe Delta MERGEs, minute-level Gold KPIs, and idempotent streaming telemetry. Replace the Unity Catalog landing-file source with Kafka/Event Hubs/Kinesis/Auto Loader for production.
# MAGIC
# MAGIC **RESULTS_DASHBOARD** queries the actual generated tables, including live sales and streaming health. It does not hard-code table counts or claim universal accuracy.
# MAGIC
# MAGIC ## Important limitations
# MAGIC This archive is a production-style demonstration, not a claim of full production readiness. Runtime compatibility, permissions, checkpoint storage, MLflow experiment access, and stream behavior must be verified on the target Databricks workspace. Real production data also requires contracts, CI/CD, environment separation, secrets, alerting, retention policies, and operational SLOs.