# Databricks notebook source
# MAGIC %md
# MAGIC # Retail AI Platform Guide — Implementation-Aligned
# MAGIC
# MAGIC ## Architecture
# MAGIC **Bronze → Silver → Gold → ML / GenAI / Dashboards**, with Data Quality and Monitoring as cross-cutting layers and a separate near-real-time stream feeding Bronze/Silver/Gold live tables.
# MAGIC
# MAGIC ## Schema inventory
# MAGIC The project uses eight schemas: `retail_bronze`, `retail_silver`, `retail_gold`, `retail_ml`, `retail_genai`, `retail_quality`, `retail_monitoring`, and `retail_metrics`.
# MAGIC
# MAGIC Do not rely on a hard-coded total table count. Use this query after execution:
# MAGIC ```sql
# MAGIC SELECT table_schema, COUNT(*) AS tables
# MAGIC FROM <catalog>.information_schema.tables
# MAGIC WHERE table_schema LIKE 'retail_%'
# MAGIC GROUP BY table_schema
# MAGIC ORDER BY table_schema;
# MAGIC ```
# MAGIC
# MAGIC ## Production-style design choices
# MAGIC - Delta tables for ACID storage and MERGE semantics
# MAGIC - Typed Silver contracts
# MAGIC - Refund-aware revenue logic
# MAGIC - Metadata-driven data quality with historical results
# MAGIC - MLflow experiment tracking with preprocessing packaged in model pipelines
# MAGIC - Read-only governed SQL and lexical knowledge retrieval for the agent
# MAGIC - Checkpointed Structured Streaming with retry-safe Bronze/Silver/Gold updates
# MAGIC - Batch and streaming operational telemetry
# MAGIC
# MAGIC ## What still needs production hardening
# MAGIC - Replace the synthetic Unity Catalog landing-file source and generated batch data with actual source systems.
# MAGIC - Add CI/CD and Databricks Declarative Automation Bundles.
# MAGIC - Use environment-specific catalogs/storage/checkpoints.
# MAGIC - Apply Unity Catalog least-privilege permissions and PII controls.
# MAGIC - Configure secrets and external connections through supported secret management.
# MAGIC - Define freshness, latency, throughput, DQ, and model-drift SLOs with alerts.
# MAGIC - Use a validated target definition and time-aware validation for real churn modeling.
# MAGIC - Promote models through the Model Registry and serving only after real-data validation.
# MAGIC
# MAGIC ## Orchestration note
# MAGIC A DBC import restores notebooks but does **not** create Lakeflow Jobs or Lakeflow pipelines. For this archive, orchestrate notebooks `00`–`05` as batch tasks and operate `06_realtime_streaming` separately for the live path. Databricks recommends Lakeflow Declarative Pipelines for new production streaming ETL.
# MAGIC
# MAGIC ## Demo guidance
# MAGIC Run `00`–`05`, then run `07_runtime_validation`. Start `06_realtime_streaming` separately for the live demo, then refresh the Real-Time Sales and Streaming Health cells in `RESULTS_DASHBOARD`. Ask `AGENT_CHAT`: **Show live sales stream**.
# MAGIC
# MAGIC ## Accuracy of claims
# MAGIC This project is a portfolio/demo implementation. Avoid claims such as “100% accurate,” “fully production ready,” or a fixed table count unless measured in the actual workspace at presentation time.