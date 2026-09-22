# Databricks notebook source
# DBTITLE 1,Project Summary (50 Lines)
# MAGIC %md
# MAGIC # RetailHub AI — Project Summary (50 Lines)
# MAGIC
# MAGIC **What is this project?**
# MAGIC RetailHub AI is an end-to-end retail data platform built on Databricks Free Edition. It demonstrates a complete lakehouse architecture — from raw data ingestion through analytics, machine learning, GenAI agents, and real-time streaming — all running on serverless compute without DBFS dependencies.
# MAGIC
# MAGIC **Architecture: Medallion Lakehouse (Bronze → Silver → Gold)**
# MAGIC 1. **Bronze**: 15 raw tables with 326K deterministic synthetic rows (customers, orders, products, payments, shipments, returns, inventory, clickstream, support tickets, etc.)
# MAGIC 2. **Silver**: Typed fact and dimension tables with refund-aware revenue, cancelled-order handling, and SCD Type 2 customer tracking
# MAGIC 3. **Gold**: Business-facing analytics — customer 360, product KPIs, daily sales, executive KPIs, store performance, inventory health, CLV, data quality scorecard
# MAGIC
# MAGIC **ML Layer**: Random Forest churn classifier, KMeans customer segmentation, Isolation Forest anomaly detection — all logged to MLflow with sklearn Pipelines
# MAGIC
# MAGIC **GenAI Layer**: Grounded agent with 100 knowledge documents, keyword retrieval, prompt injection filtering, and 9 tools (sales, customer 360, inventory, churn, anomalies, DQ, pipeline status, executive KPIs, ML models)
# MAGIC
# MAGIC **Streaming Layer**: Serverless-safe near-real-time streaming using Trigger.AvailableNow() with Delta MERGE into Bronze/Silver/Gold, plus a Lakeflow pipeline source using pyspark.pipelines and Auto Loader
# MAGIC
# MAGIC **Quality Layer**: 25+ metadata-driven rules executed dynamically from a quality_rules table — uniqueness, completeness, validity, referential integrity, timeliness, consistency
# MAGIC
# MAGIC **Monitoring**: Pipeline run logs, streaming metrics with event lag, anomaly events
# MAGIC
# MAGIC **Validation**: 20+ end-to-end checks — object existence, FK integrity, financial reconciliation, DQ execution, ML coverage, streaming output
# MAGIC
# MAGIC **Key Design Decisions**:
# MAGIC - Serverless-only: no .cache(), no processingTime, no DBFS root — all Unity Catalog managed
# MAGIC - Parameterized catalog via widgets with input validation
# MAGIC - Deterministic data (seed=42) for reproducible demos
# MAGIC - Metadata-driven quality (rules stored as data, not hardcoded)
# MAGIC - Grounded agent (no LLM hallucination — answers come from SQL queries against lakehouse tables)
# MAGIC - AvailableNow streaming (finite batches, serverless-safe, re-runnable)
# MAGIC
# MAGIC **Deployment**: Companion DAB (Declarative Automation Bundle) creates the E2E Job, serverless Lakeflow pipeline, MLflow experiment, and optional continuous pipeline job (paused by default)
# MAGIC
# MAGIC **Folder**: `retail-ai-free-edition-workspace-clean-import-v4.2` | **Version**: 4.0 | **Date**: 2026-09-21

# COMMAND ----------

# DBTITLE 1,00_START_HERE_FREE_EDITION
# MAGIC %md
# MAGIC ## 1. `00_START_HERE_FREE_EDITION` — Entry Point & Run Guide
# MAGIC
# MAGIC ### What it does
# MAGIC This is the project README in notebook form. It explains the platform boundary (a DBC imports notebooks but cannot create Jobs or pipelines), the manual run order (00→07), and how to use the companion DAB for full deployment.
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC Databricks Free Edition users importing a DBC need a clear starting point that explains what they can and cannot do. The notebook format ensures the guide is visible in the same workspace as the code, not buried in a separate README file. The explicit run order prevents users from executing notebooks out of sequence (e.g., running quality checks before any data exists). The platform boundary note manages expectations — you can run all notebooks manually, but automating them as a Job requires the DAB.
# MAGIC
# MAGIC ### Key concepts
# MAGIC - **DBC vs DAB**: A DBC is a notebook archive; a DAB is infrastructure-as-code that creates Jobs, pipelines, and MLflow experiments
# MAGIC - **Serverless constraint**: Free Edition supports serverless compute only — no classic clusters
# MAGIC - **Manual run order**: 00_setup → 01_bronze → 02_silver_gold → 03_dq → 04_ml → 05_genai → 06_streaming → 07_validation

# COMMAND ----------

# DBTITLE 1,00_setup
# MAGIC %md
# MAGIC ## 2. `00_setup` — Schema, Quality Rules & Volume Initialization
# MAGIC
# MAGIC ### What it does
# MAGIC Creates 9 Unity Catalog schemas (`retail_bronze`, `retail_silver`, `retail_gold`, `retail_ml`, `retail_genai`, `retail_quality`, `retail_monitoring`, `retail_metrics`, `retail_realtime`). Populates a `quality_rules` table with 25+ metadata-driven rules. Creates `pipeline_runs` and `streaming_metrics` monitoring tables. Creates a UC managed volume `retail_ai_files` for streaming checkpoints and landing files.
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Parameterized catalog**: Uses `dbutils.widgets` with regex validation instead of hardcoding `"workspace"`. This makes the project portable across workspaces — you can override the catalog without editing code.
# MAGIC - **Metadata-driven quality rules**: Rules are stored as data in a table (with `check_expression` SQL), not hardcoded in Python. This means you can add/modify/disable rules by updating the table, without touching the quality notebook. Each rule has severity (critical/error/warning/informational), threshold, and an executable SQL expression.
# MAGIC - **UC managed volume**: Free Edition doesn't support DBFS root. A Unity Catalog managed volume provides governed storage for streaming file landing and checkpoints — serverless-safe and permission-controlled.
# MAGIC - **9 schemas (not 8)**: The `retail_realtime` schema was added for the Lakeflow pipeline's managed streaming outputs, separating them from batch Gold tables.
# MAGIC - **Streaming metrics table**: Includes `event_lag_seconds` for tracking real-time freshness — a dimension missing from basic pipeline monitoring.

# COMMAND ----------

# DBTITLE 1,01_bronze_ingestion
# MAGIC %md
# MAGIC ## 3. `01_bronze_ingestion` — Synthetic Data Generation (326K rows, 15 tables)
# MAGIC
# MAGIC ### What it does
# MAGIC Generates deterministic synthetic retail data across 15 Bronze tables: customers (10K), orders (30K), order_items (75K), products (1K), categories (8), payments (32K), shipments (28K), returns (5K), inventory_events (40K), stores (50), suppliers (150), clickstream_events (100K), support_tickets (3K), product_reviews, and knowledge_documents (100).
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Deterministic seed (SEED=42)**: Every run produces the same data, making demos reproducible and test assertions reliable. The `rng(i, offset)` function creates per-row random generators so each row is unique but deterministic.
# MAGIC - **Date-anchored to current_date()**: Order dates, registration dates, and shipment dates are relative to the Spark session's current date, so the data always looks "fresh" regardless of when you run it.
# MAGIC - **Realistic constraints enforced during generation**:
# MAGIC   - Cancelled orders are never shipped (business rule)
# MAGIC   - Every Returned order has an approved return record (referential consistency)
# MAGIC   - Inventory starts with opening stock and cannot go negative (physical constraint)
# MAGIC   - Refunds are capped to purchased net value (financial integrity)
# MAGIC   - Verified reviews are tied to completed/returned purchases (review integrity)
# MAGIC   - Age-aware order states (orders have realistic lifecycles)
# MAGIC - **Audit columns**: Every row gets `_batch_id`, `_run_id`, and `_ingestion_ts` for lineage tracking — essential for understanding which run produced which data.
# MAGIC - **overwriteSchema=true**: Allows schema evolution between runs without manual table drops.

# COMMAND ----------

# DBTITLE 1,02_silver_gold_pipeline
# MAGIC %md
# MAGIC ## 4. `02_silver_gold_pipeline` — Silver Facts/Dimensions & Gold Analytics
# MAGIC
# MAGIC ### What it does
# MAGIC Transforms Bronze into typed Silver fact/dimension tables (`fact_orders`, `fact_order_items`, `fact_payments`, `fact_returns`, `fact_shipments`, `dim_customers`, `dim_products`, `dim_stores`) and Gold analytics products (`customer_360`, `product_kpis`, `daily_sales_summary`, `executive_kpi`, `store_performance`, `inventory_health`, `customer_lifetime_value`, `data_quality_kpi`, `quality_scorecard`).
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Refund-aware revenue**: Net revenue = order_total - discount_amount - refund_amount. Cancelled orders recognize zero revenue. This prevents overstatement of revenue in analytics.
# MAGIC - **Many-to-many inflation prevention**: Product KPIs aggregate sales, reviews, and refunds independently (separate sub-aggregations joined on product_id) to avoid the classic fan-out problem where joining order_items to reviews multiplies rows.
# MAGIC - **Explicit date typing**: Dates are cast to `DATE` type during transformation, not left as strings, so downstream SQL date functions work correctly.
# MAGIC - **SCD Type 2 for dim_customers**: Customer dimension tracks changes over time (segment changes, status changes) with effective_from/effective_to columns — essential for historical analysis.
# MAGIC - **customer_360 as a single denormalized view**: Joins customer profile, order history, payment history, return history, churn risk, and CLV tier into one table — optimized for the agent's `get_customer_360` tool to avoid multi-table joins at query time.
# MAGIC - **Separation of batch Gold vs streaming Gold**: `live_sales_minute` (streaming) is kept separate from `daily_sales_summary` (batch) so streaming increments don't rebuild historical tables.

# COMMAND ----------

# DBTITLE 1,03_data_quality_checks
# MAGIC %md
# MAGIC ## 5. `03_data_quality_checks` — Metadata-Driven Quality Framework
# MAGIC
# MAGIC ### What it does
# MAGIC Reads all enabled rules from `retail_quality.quality_rules`, dynamically executes each rule's `check_expression` SQL against Bronze tables, computes violation counts and failure percentages, writes results to `quality_results` and `quality_results_history`, and creates a `quality_summary` aggregation by severity.
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Metadata-driven (rules as data)**: The quality rules are stored in a table with executable SQL expressions, not hardcoded in Python if/else blocks. This means:
# MAGIC   - New rules can be added by inserting rows into the table
# MAGIC   - Rules can be disabled by setting `enabled=false`
# MAGIC   - Thresholds can be tuned without code changes
# MAGIC   - The quality engine code never changes — only the data does
# MAGIC - **Separation of results and history**: `quality_results` is overwritten each run (latest state), while `quality_results_history` appends (trend tracking). This supports both operational monitoring (current status) and analytical monitoring (quality trends over time).
# MAGIC - **Error handling per rule**: If a rule's SQL fails (e.g., table doesn't exist), it's marked as ERROR (not FAIL) with the error message truncated to 200 chars. This distinguishes execution failures from data quality failures.
# MAGIC - **Failure percentage with threshold**: Each rule has a threshold (e.g., 0 for critical, 5.0 for warnings). The status is PASS if `failure_pct <= threshold`, FAIL otherwise. This allows tolerances — e.g., 2% invalid emails is a warning, not a hard failure.
# MAGIC - **Quality summary by severity**: Aggregates results into critical/error/warning/informational buckets with pass rates — feeds the `data_quality_kpi` Gold table and the agent's `get_data_quality_results` tool.
# MAGIC - **Primary key constraint**: A primary key on `customers.customer_id` was added so the optimizer can skip redundant grouping aggregates in `COUNT(DISTINCT)` queries (performance insight fix).

# COMMAND ----------

# DBTITLE 1,04_ml_training
# MAGIC %md
# MAGIC ## 6. `04_ml_training` — Churn, Segmentation & Anomaly Models with MLflow
# MAGIC
# MAGIC ### What it does
# MAGIC Trains three ML models using sklearn Pipelines:
# MAGIC 1. **Churn classifier** (Random Forest): Predicts customer churn probability using features from `retail_ml.features` (order frequency, recency, revenue, returns, support tickets, etc.)
# MAGIC 2. **Customer segmentation** (KMeans): Groups customers into 4 clusters (Low/Medium/High Value, Champions) using scaled features
# MAGIC 3. **Anomaly detection** (Isolation Forest): Identifies outlier transactions based on order patterns
# MAGIC
# MAGIC All models are logged to a per-user MLflow experiment with sklearn Pipelines (preprocessing + model bundled together).
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **sklearn Pipelines (not bare models)**: Preprocessing (StandardScaler) is packaged inside the Pipeline, so the saved model includes preprocessing. This means predictions on new data don't need separate preprocessing steps — the Pipeline handles it.
# MAGIC - **Per-user MLflow experiment** (`/Users/{user}/retail-ai-free-edition`): Avoids dependency on shared workspace folders. Each user gets their own experiment, preventing permission issues in Free Edition.
# MAGIC - **MLflow log_model with input_example**: Provides schema inference for the model, making it easier to deploy and understand expected inputs.
# MAGIC - **positive_class_probability helper**: Extracts the probability of churn (class 1) regardless of class label ordering — handles cases where sklearn assigns class labels in unexpected order.
# MAGIC - **Deterministic demand forecast**: The demand output is a 90-day daily-average baseline, not a calibrated forecasting model. This is honest about what the model does — it's a demo, not production forecasting.
# MAGIC - **Silhouette score for KMeans**: Evaluates cluster quality objectively, not just inertia. Silhouette accounts for both cohesion and separation.
# MAGIC - **high_risk_customers table**: Pre-filters churn predictions >= 0.7 into a separate table — the agent's `get_high_churn_customers` tool queries this for fast retrieval.

# COMMAND ----------

# DBTITLE 1,05_genai_agent
# MAGIC %md
# MAGIC ## 7. `05_genai_agent` — Grounded Knowledge Base & Tool-Using Agent
# MAGIC
# MAGIC ### What it does
# MAGIC Builds a GenAI agent system with two main components:
# MAGIC 1. **Knowledge Base**: Indexes 100 knowledge documents (policies, procedures, FAQs) into `document_chunks` with keyword-based retrieval (no external vector API needed)
# MAGIC 2. **Agent System**: A tool-using agent with prompt injection filtering, 6 specialized tools, and an interaction log
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Keyword retrieval (not vector search)**: Free Edition may not have vector search endpoints. SQL-based keyword retrieval using `LIKE` and `CONTAINS` works on any Databricks setup without external APIs. It's less sophisticated than embeddings but sufficient for a demo and requires no infrastructure.
# MAGIC - **Prompt injection filter**: The agent checks user queries for injection attempts before processing. This is a security measure — prevents users from bypassing the agent's tool restrictions via crafted prompts.
# MAGIC - **6 safe tools (SQL, Retrieval, Metric, Alert, Status, Report)**: Each tool is a Python function that queries specific Gold/ML/Monitoring tables. The agent doesn't generate free-form SQL — it routes to pre-defined tools with parameterized queries. This prevents SQL injection and ensures responses are grounded in actual data.
# MAGIC - **Schema-aligned tools**: Each tool queries tables that actually exist in the pipeline output (customer_360, daily_sales_summary, inventory_health, quality_results, pipeline_runs, etc.). No phantom queries against non-existent tables.
# MAGIC - **Interaction log**: Every agent interaction is logged to `agent_interactions` with the query, tool used, response, and timestamp — enables auditing and evaluation.
# MAGIC - **Evaluation table**: Includes a routing/source-support evaluation table to measure how accurately the agent routes questions to the right tool — a built-in quality metric for the agent itself.

# COMMAND ----------

# DBTITLE 1,06_realtime_streaming
# MAGIC %md
# MAGIC ## 8. `06_realtime_streaming` — Near-Real-Time Streaming with AvailableNow
# MAGIC
# MAGIC ### What it does
# MAGIC Implements serverless-safe near-real-time streaming:
# MAGIC 1. Generates a finite batch of realistic order events (default 500) into a UC Volume
# MAGIC 2. Reads only new files via Structured Streaming with `Trigger.AvailableNow()`
# MAGIC 3. Processes all available events using `foreachBatch` with Delta MERGE into Bronze/Silver/Gold
# MAGIC 4. Records streaming health metrics (event lag, processing time, row counts)
# MAGIC 5. Exits cleanly — re-run to simulate the next increment
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Trigger.AvailableNow() (not processingTime)**: Free Edition serverless notebooks don't support `processingTime` triggers. `AvailableNow` processes all currently available files and then exits — perfect for serverless compute that spins up and down. This is the key Free Edition adaptation.
# MAGIC - **No .cache() (serverless constraint)**: The `process_microbatch` function originally used `.cache()` on DataFrames, but `PERSIST TABLE` is not supported on serverless compute. The fix removes `.cache()` — the batch is small (500 events) so re-computation is negligible.
# MAGIC - **Delta MERGE (not append)**: Uses `MERGE INTO` with `ON t.event_id=s.event_id` for idempotency. Re-running with the same events won't create duplicates — the MERGE updates existing rows or inserts new ones.
# MAGIC - **Persistent checkpoint**: The streaming checkpoint lives in the UC Volume, so each run only processes new files. Without checkpoints, every run would reprocess all files.
# MAGIC - **Gold recompute (not append)**: For the Gold `live_sales_minute` table, the notebook identifies affected minute/channel keys from the current batch, then recomputes those specific partitions from Silver — not a full table rebuild. This is incremental but consistent.
# MAGIC - **Event lag tracking**: Calculates the difference between processing time and event time — a real-time freshness metric. If lag grows, it indicates the pipeline can't keep up with incoming events.

# COMMAND ----------

# DBTITLE 1,06B_lakeflow_pipeline_source
# MAGIC %md
# MAGIC ## 9. `06B_lakeflow_pipeline_source` — Lakeflow Pipeline Source (pyspark.pipelines)
# MAGIC
# MAGIC ### What it does
# MAGIC This is NOT a standalone notebook — it's source code for a Lakeflow Spark Declarative Pipeline. It uses `pyspark.pipelines` (available only in pipeline context) to define:
# MAGIC 1. `orders_bronze_stream` — streaming table with Auto Loader reading JSON from UC Volume, with 5 data quality expectations
# MAGIC 2. `orders_silver_stream` — streaming table with net_revenue calculation and positive_net_revenue expectation
# MAGIC 3. `live_sales_minute_pipeline` — materialized view with minute/channel sales KPIs
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **pyspark.pipelines (not dlt)**: Uses the newer `pyspark.pipelines` API (the Python SDK for Spark Declarative Pipelines) instead of the older `dlt` module. This is the forward-compatible path.
# MAGIC - **Auto Loader (cloudFiles)**: Uses `spark.readStream.format("cloudFiles")` for incremental file ingestion from the UC Volume. Auto Loader automatically discovers new files and tracks them via checkpoints — no manual file listing needed.
# MAGIC - **Expectations (expect_or_drop)**: Data quality is built into the pipeline. Invalid events (null event_id, invalid channel, negative amounts) are dropped before reaching Silver. This is pipeline-native quality, separate from the metadata-driven quality in notebook 03.
# MAGIC - **Materialized view for Gold**: `live_sales_minute_pipeline` is a `@dp.materialized_view`, not a streaming table. Materialized views are recomputed on each pipeline update — they can aggregate over the full Silver history, not just new rows. This is correct for minute-level KPIs that need to reflect all data, not just the latest batch.
# MAGIC - **Do not run directly**: The notebook imports `pyspark.pipelines` which is only available in pipeline context. Running it as a normal notebook will fail. It's deployed via the DAB as a serverless pipeline.
# MAGIC - **Shared volume with notebook 06**: Both the notebook streaming (06) and the Lakeflow pipeline (06B) read from the same UC Volume landing path. This lets you switch between manual notebook streaming and managed pipeline streaming without changing the data source.

# COMMAND ----------

# DBTITLE 1,07_runtime_validation
# MAGIC %md
# MAGIC ## 10. `07_runtime_validation` — End-to-End Validation (20+ checks)
# MAGIC
# MAGIC ### What it does
# MAGIC Validates the entire pipeline output with 20+ checks across 6 categories:
# MAGIC 1. **Object existence**: Verifies all expected tables exist (bronze, silver, gold, ml, genai, quality)
# MAGIC 2. **Referential integrity**: FK checks (orders→customers, items→orders, returns→items, payments→orders, shipments→orders, tickets→customers)
# MAGIC 3. **Financial consistency**: Order total vs sum of line items, cancelled orders have no completed payments, returned orders have approved returns
# MAGIC 4. **Data quality execution**: No ERROR status in quality_results
# MAGIC 5. **ML coverage**: Churn predictions cover all customers
# MAGIC 6. **Streaming output**: Realtime tables have data, Gold live_sales_minute has KPIs
# MAGIC Also validates Lakeflow pipeline outputs if the managed pipeline has run.
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Validation as data**: Results are stored in a `validation_results` table with pass/fail status, actual vs expected values, and error details — not just printed to console. This enables historical tracking and alerting.
# MAGIC - **Comprehensive business rules**: The checks go beyond schema validation — they enforce business logic:
# MAGIC   - Cancelled orders must NOT have completed payments (financial integrity)
# MAGIC   - Returned orders MUST have approved return records (operational consistency)
# MAGIC   - Inventory stock must never be negative (physical constraint)
# MAGIC   - Shipments can't be delivered before shipping or after today (temporal logic)
# MAGIC - **Designed for CI/CD**: The validation notebook can be the final task in a Job — if any check fails, the job fails. This makes it a gate for production deployments.
# MAGIC - **Lakeflow pipeline awareness**: If the DAB pipeline has run, it validates those outputs too. If not, it skips them gracefully — no hard dependency.

# COMMAND ----------

# DBTITLE 1,AGENT_CHAT
# MAGIC %md
# MAGIC ## 11. `AGENT_CHAT` — Interactive Chat Interface
# MAGIC
# MAGIC ### What it does
# MAGIC Provides an interactive chat UI inside a notebook. Users type a question in a text widget, and the agent routes it to one of 9 tools:
# MAGIC 1. `search_knowledge` — Retrieve relevant documentation from the knowledge base
# MAGIC 2. `get_sales_summary` — Daily/monthly revenue trends from `daily_sales_summary`
# MAGIC 3. `get_customer_360` — Complete customer profiles from `customer_360`
# MAGIC 4. `get_inventory_risk` — Stock-out predictions from `inventory_health`
# MAGIC 5. `get_high_churn_customers` — High-risk customers from `churn_predictions` joined with `customer_360`
# MAGIC 6. `get_anomaly_details` — Outlier transactions from `anomaly_events`
# MAGIC 7. `get_data_quality_results` — DQ status from `quality_summary`
# MAGIC 8. `get_pipeline_status` — Monitoring & alerts from `pipeline_runs`
# MAGIC 9. `get_executive_kpi` — Executive dashboard from `executive_kpi`
# MAGIC 10. `get_ml_model_summary` — ML model performance summary
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Notebook-based (no app deployment)**: Free Edition users may not want to deploy a separate web app. A notebook chat interface works immediately — just run the cell.
# MAGIC - **Keyword-based routing**: The agent uses keyword matching to route questions to tools (e.g., "revenue" → get_sales_summary, "churn" → get_high_churn_customers). Simple but effective for a demo — no LLM needed for routing.
# MAGIC - **Formatted markdown responses**: Tools return markdown tables and formatted text, not raw DataFrames. This makes responses readable directly in the notebook output.
# MAGIC - **Grounded (no hallucination)**: Every response comes from a SQL query against actual lakehouse tables. The agent never generates free-form text — it formats query results. This is the key design principle: the agent is a query orchestrator, not a language model.
# MAGIC - **All queries use the parameterized catalog**: Tools use the `CAT` variable, so the chat works regardless of which catalog the user selected.

# COMMAND ----------

# DBTITLE 1,RESULTS_DASHBOARD
# MAGIC %md
# MAGIC ## 12. `RESULTS_DASHBOARD` — Batch & Streaming Analytics Dashboard
# MAGIC
# MAGIC ### What it does
# MAGIC Displays the project's analytical output in a series of `display()` calls:
# MAGIC - Executive KPIs (total customers, orders, revenue, returns, products, tickets)
# MAGIC - Daily sales summary by channel
# MAGIC - Customer 360 highlights (top customers by revenue, churn risk distribution)
# MAGIC - Product KPIs (top products, low performers)
# MAGIC - Store performance
# MAGIC - Inventory health (stock status distribution)
# MAGIC - Data quality scorecard
# MAGIC - ML model summary (churn distribution, segment sizes, demand forecast)
# MAGIC - Near-real-time streaming metrics (live sales by minute, streaming health)
# MAGIC - Pipeline run history
# MAGIC
# MAGIC ### Why it's designed this way
# MAGIC - **Live queries (not cached)**: Every `display()` call runs a fresh SQL query against the Gold/ML/Monitoring tables. This means the dashboard always reflects the latest data — re-run `06_realtime_streaming` and refresh to see new streaming results.
# MAGIC - **Streaming + batch in one view**: The dashboard shows both batch analytics (daily sales, customer 360) and streaming analytics (live sales by minute) side by side, demonstrating the dual-path architecture.
# MAGIC - **No custom visualization code**: Uses Databricks built-in `display()` rendering, which auto-detects chart types and allows interactive exploration. No matplotlib/plotly dependency — works on serverless without library installation.
# MAGIC - **Run-order dependency**: The dashboard expects all numbered notebooks (00-07) to have run first. If tables don't exist, the display calls will error — this is intentional, not a bug. The notebook header explicitly says to run the pipeline first.
# MAGIC - **33 cells of analytics**: The dashboard is comprehensive — it covers every layer of the lakehouse, giving a full picture of what the platform produces.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## v4.2 Industry-Readiness Update
# MAGIC The Free Edition project now standardizes Spark timestamps to UTC, records duration/project version/environment in pipeline monitoring, blocks critical/error data-quality failures, captures DQ failure samples, and uses clearer semantic names including `fact_inventory_daily_movement`, `baseline_3yr_clv`, and `recency_risk`.
# MAGIC
# MAGIC ML training compares multiple churn classifiers, removes the direct recency leakage feature from churn training, evaluates K-Means cluster counts using silhouette score, and bases anomaly severity on anomaly score. The streaming path remains Free Edition-safe with `Trigger.AvailableNow()`, insert-only Bronze event handling, event-time ordering in Silver, and operational telemetry.
# MAGIC
# MAGIC Runtime validation now includes cross-layer reconciliation and blocking failures for core integrity checks. These are production-style patterns demonstrated within Databricks Free Edition constraints; real production deployment still requires real source systems, CI/CD, environment separation, permissions, secrets, SLOs, and real-data model validation.
