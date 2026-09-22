# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 · Declarative Automation Bundle (DAB) Deployment Guide
# MAGIC The DBC and DAB serve different purposes: **DBC = notebook import**, **DAB = resource deployment**. Use the companion ZIP `retail-ai-free-edition-dab-v4.0.zip` when you want Databricks to create/update Jobs and the Lakeflow pipeline automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy
# MAGIC From a machine with a current Databricks CLI, authenticate to your Free Edition workspace, unzip the bundle, then run:
# MAGIC 
# MAGIC ```bash
# MAGIC databricks bundle validate -t free
# MAGIC databricks bundle deploy -t free
# MAGIC databricks bundle run -t free retail_ai_e2e_job
# MAGIC ```
# MAGIC 
# MAGIC The E2E Job runs the project sequentially and includes one AvailableNow streaming increment plus a triggered Lakeflow pipeline refresh before runtime validation.
# MAGIC 
# MAGIC The bundle also deploys `retail_ai_continuous_pipeline_job` in **PAUSED** state. Unpause it only when you specifically want an always-on managed stream and have enough Free Edition quota.

# COMMAND ----------

# DBTITLE 1,DAB Deployment Guide - Recommendations
# MAGIC %md
# MAGIC ## Existing pipeline
# MAGIC If you already created a Lakeflow pipeline and want the bundle to manage that same resource instead of creating a second one, use Databricks bundle resource binding after deployment/generation. Keep only one active realtime pipeline path at a time to avoid two consumers writing competing demo outputs.
# MAGIC 
# MAGIC ## Recommended submission/demo
# MAGIC Use the DAB-managed E2E job for repeatability. Show `RESULTS_DASHBOARD`, `AGENT_CHAT`, MLflow runs, data-quality results, the Lakeflow pipeline graph, and `07_runtime_validation` as evidence that the pieces connect end to end.
# MAGIC 
# MAGIC ## Tier-1 and Tier-2 industry-readiness improvements included
# MAGIC All notebooks in this DBC include the following industry-readiness enhancements:
# MAGIC - **UTC timezone** (`spark.sql.session.timeZone = UTC`) for consistent timestamps
# MAGIC - **Enhanced monitoring** with `duration_seconds`, `project_version`, `environment` in `pipeline_runs`
# MAGIC - **DQ blocking logic**: critical/error failures halt the pipeline
# MAGIC - **DQ failure samples** for root-cause analysis
# MAGIC - **Semantic naming**: `fact_inventory_daily_movement`, `baseline_3yr_clv`, `recency_risk`
# MAGIC - **Governance comments** on all tables
# MAGIC - **Free Edition constraints**: no GPU/deep learning; serverless `Trigger.AvailableNow()` only
