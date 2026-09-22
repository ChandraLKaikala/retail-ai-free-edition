# Databricks notebook source
# MAGIC %md
# MAGIC # START HERE · RetailHub AI — Databricks Free Edition E2E v4.0
# MAGIC This archive is the **Free Edition-safe notebook package**. It uses Python, Unity Catalog managed tables/volumes, serverless-compatible ML/MLflow, metadata-driven quality, a grounded local agent, an incremental Structured Streaming demo using `Trigger.AvailableNow()`, and a Lakeflow pipeline source notebook.
# MAGIC
# MAGIC ## Important platform boundary
# MAGIC A DBC imports notebooks only. It cannot itself create Lakeflow Jobs or pipeline resources. The companion `retail-ai-free-edition-dab-v4.0.zip` is the deployable Declarative Automation Bundle that creates the Job, serverless Lakeflow pipeline, MLflow experiment, and optional continuous pipeline job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fastest manual run in Free Edition
# MAGIC 1. Import this DBC into your workspace.
# MAGIC 2. Run `00_setup`.
# MAGIC 3. Run `01_bronze_ingestion`.
# MAGIC 4. Run `02_silver_gold_pipeline`.
# MAGIC 5. Run `03_data_quality_checks`.
# MAGIC 6. Run `04_ml_training`.
# MAGIC 7. Run `05_genai_agent`.
# MAGIC 8. Run `06_realtime_streaming` — it lands a finite event batch, consumes it with `Trigger.AvailableNow()`, and exits.
# MAGIC 9. Run `07_runtime_validation`.
# MAGIC 10. Open `RESULTS_DASHBOARD` and `AGENT_CHAT`.
# MAGIC
# MAGIC Re-run `06_realtime_streaming` whenever you want another incremental near-real-time batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Free Edition design choices
# MAGIC - **Serverless only:** no cluster definitions are required.
# MAGIC - **Python only:** no R/Scala dependency.
# MAGIC - **No DBFS root:** landing data and checkpoints use `/Volumes/<catalog>/retail_monitoring/retail_ai_files/...`.
# MAGIC - **Streaming:** notebook/job mode uses `Trigger.AvailableNow()`; no `processingTime` trigger.
# MAGIC - **Lakeflow:** `06B_lakeflow_pipeline_source` uses `pyspark.pipelines` and Auto Loader for the managed path.
# MAGIC - **Quota-aware:** the DAB main workflow is sequential, so it does not require more than one concurrent task. The optional continuous pipeline Job is deployed paused to avoid consuming Free Edition quota accidentally.
# MAGIC - **No external APIs required:** the agent and generated data work without unrestricted outbound internet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is included
# MAGIC **Data engineering:** Bronze/Silver/Gold Delta tables, current synthetic data, referentially coherent orders/items/payments/shipments/returns/inventory/reviews.
# MAGIC
# MAGIC **Quality:** metadata-driven checks, severity/thresholds, latest results, history, runtime validation.
# MAGIC
# MAGIC **ML/MLOps:** churn, K-Means segmentation, Isolation Forest anomalies, deterministic demand baseline, MLflow tracking.
# MAGIC
# MAGIC **AI:** grounded tool-routing assistant, knowledge retrieval, read-only SQL guardrails, safety filtering, evaluation and chat notebook.
# MAGIC
# MAGIC **Realtime:** Free Edition AvailableNow stream + optional managed Lakeflow pipeline.
# MAGIC
# MAGIC **Observability:** pipeline runs, streaming metrics, dashboard, validation outputs.
# MAGIC
# MAGIC **Deployment:** companion DAB defines the serverless E2E Job, serverless pipeline, MLflow experiment, triggered pipeline refresh, and paused continuous pipeline job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed documentation included in this DBC
# MAGIC - `DETAILED_PROJECT_SUMMARY` — architecture, logic, tables, Free Edition design, DABs, limitations, and run flow.
# MAGIC - `CELL_BY_CELL_CODE_EXPLANATION` — every notebook/cell explained using What / Why / How.
# MAGIC - `DETAILED_PRESENTATION_WHAT_WHY_HOW` — 32-slide presentation script with speaker notes and Q&A talking points.
