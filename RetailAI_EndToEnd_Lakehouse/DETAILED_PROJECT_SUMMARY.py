# Databricks notebook source
# MAGIC %md
# MAGIC # RetailHub AI — Detailed End-to-End Project Summary
# MAGIC
# MAGIC **Package:** Databricks Free Edition E2E v4.0  
# MAGIC **Purpose:** A complete retail lakehouse demonstration combining governed data engineering, data quality, ML/MLOps, grounded GenAI, near-real-time processing, observability, runtime validation, and Databricks Declarative Automation Bundles (DABs).
# MAGIC
# MAGIC ## 1. Executive Summary
# MAGIC
# MAGIC RetailHub AI is an end-to-end Databricks project that simulates a realistic retail platform and carries data through the complete lifecycle from raw operational records to business analytics, machine learning, AI-assisted querying, streaming KPIs, monitoring, and deployment automation.
# MAGIC
# MAGIC The project is deliberately designed for **Databricks Free Edition**. That changes several architectural choices: it avoids classic-cluster assumptions, avoids DBFS-root storage dependencies, uses Unity Catalog tables/volumes, uses serverless-safe `Trigger.AvailableNow()` for notebook/job streaming, and includes a separate Lakeflow pipeline source for the managed serverless streaming path. The companion DAB package defines the Jobs, pipeline, schemas, volume, and MLflow experiment that a DBC archive cannot create on its own.
# MAGIC
# MAGIC The project should be described as a **production-pattern demonstration**, not as a production SLA-backed system. The code is designed to demonstrate correct architecture and engineering practices within Free Edition constraints. Final runtime verification must still occur inside the target Databricks workspace because catalog permissions, quotas, runtime versions, and workspace features are external to the DBC file.
# MAGIC
# MAGIC ## 2. Business Problem
# MAGIC
# MAGIC A modern retailer has operational data spread across customers, orders, products, payments, stores, suppliers, shipments, returns, inventory, web activity, customer support, reviews, and enterprise documents. When those sources are handled independently, several problems appear:
# MAGIC
# MAGIC - revenue definitions differ across teams;
# MAGIC - cancelled orders or refunds can be counted incorrectly;
# MAGIC - product metrics can be inflated by bad joins;
# MAGIC - data-quality problems are discovered late;
# MAGIC - ML preprocessing may not match serving-time preprocessing;
# MAGIC - AI assistants may answer from ungoverned or mismatched data;
# MAGIC - dashboards can become stale;
# MAGIC - streaming demos can claim “real time” without actually executing streaming logic;
# MAGIC - deployment may depend on manual clicks rather than repeatable infrastructure definitions.
# MAGIC
# MAGIC RetailHub AI addresses these problems by putting all downstream consumers on the same governed lakehouse and using one consistent set of business definitions.
# MAGIC
# MAGIC ## 3. End-to-End Architecture
# MAGIC
# MAGIC ```text
# MAGIC Operational / Synthetic Retail Sources
# MAGIC                 |
# MAGIC                 v
# MAGIC         Bronze Delta Layer
# MAGIC   raw + auditable operational tables
# MAGIC                 |
# MAGIC                 v
# MAGIC         Silver Trusted Layer
# MAGIC  typed dimensions + facts + business logic
# MAGIC                 |
# MAGIC                 v
# MAGIC          Gold Data Products
# MAGIC  KPIs / Customer 360 / Inventory / Support
# MAGIC       /             |              \
# MAGIC      v              v               v
# MAGIC  Data Quality     ML / MLflow      GenAI
# MAGIC      |              |               |
# MAGIC      +--------------+---------------+
# MAGIC                     |
# MAGIC                     v
# MAGIC           Dashboard / Agent Chat
# MAGIC
# MAGIC Near-real-time branch:
# MAGIC Unity Catalog Volume landing files
# MAGIC         -> Structured Streaming AvailableNow
# MAGIC         -> Bronze realtime events
# MAGIC         -> Silver realtime orders
# MAGIC         -> Gold minute KPIs
# MAGIC         -> streaming_metrics
# MAGIC
# MAGIC Managed realtime branch:
# MAGIC DAB -> Serverless Lakeflow pipeline
# MAGIC         -> Auto Loader / pipeline source
# MAGIC         -> streaming tables
# MAGIC         -> materialized minute KPI view
# MAGIC ```
# MAGIC
# MAGIC ## 4. Why the Project Uses Both a DBC and a DAB
# MAGIC
# MAGIC A **DBC** is convenient for importing notebooks into the Databricks workspace. It contains the runnable notebooks and documentation notebooks. It does **not** create Jobs, Lakeflow pipelines, MLflow experiments, schemas, or volumes as deployable infrastructure resources.
# MAGIC
# MAGIC The **Declarative Automation Bundle (DAB)** is the infrastructure-as-code companion. It defines:
# MAGIC
# MAGIC - nine Unity Catalog schemas;
# MAGIC - one managed Unity Catalog volume;
# MAGIC - one per-user MLflow experiment;
# MAGIC - one sequential end-to-end serverless Job;
# MAGIC - one serverless Lakeflow pipeline;
# MAGIC - one on-demand pipeline refresh Job;
# MAGIC - one optional continuous pipeline Job deployed paused.
# MAGIC
# MAGIC This separation is intentional: the DBC is the easiest way to inspect/run notebooks manually; the DAB is the repeatable deployment mechanism.
# MAGIC
# MAGIC ## 5. Free Edition Design Decisions
# MAGIC
# MAGIC The project uses the following Free Edition-oriented decisions:
# MAGIC
# MAGIC 1. **Serverless-friendly execution.** No classic cluster configuration is required in the notebooks or bundle.
# MAGIC 2. **Unity Catalog first.** Data is stored in catalog schemas and a managed volume rather than DBFS root.
# MAGIC 3. **Bounded streaming in notebooks/jobs.** `06_realtime_streaming` uses `Trigger.AvailableNow()` so each run processes all new files and exits cleanly.
# MAGIC 4. **Managed Lakeflow option.** `06B_lakeflow_pipeline_source` is evaluated by a serverless Lakeflow pipeline through the DAB.
# MAGIC 5. **Quota awareness.** The main job is sequential and the optional continuous pipeline job is paused by default.
# MAGIC 6. **Python only.** The implementation does not require Scala or R.
# MAGIC
# MAGIC ## 6. Project Schemas
# MAGIC
# MAGIC `00_setup` creates or ensures these schemas inside the selected catalog:
# MAGIC
# MAGIC | Schema | Purpose |
# MAGIC |---|---|
# MAGIC | `retail_bronze` | Raw operational and streaming events |
# MAGIC | `retail_silver` | Trusted facts and dimensions |
# MAGIC | `retail_gold` | Business-facing analytical data products |
# MAGIC | `retail_ml` | Model predictions, segments, anomalies, demand baseline |
# MAGIC | `retail_genai` | Knowledge index, interactions, evaluation |
# MAGIC | `retail_quality` | Quality rules, latest results, history, summaries |
# MAGIC | `retail_monitoring` | Pipeline runs, streaming metrics, validation, anomaly events |
# MAGIC | `retail_metrics` | Reserved metrics namespace for extension |
# MAGIC | `retail_realtime` | Managed Lakeflow realtime outputs |
# MAGIC
# MAGIC A managed volume named `retail_ai_files` is created in `retail_monitoring` for governed landing files and Structured Streaming checkpoints.
# MAGIC
# MAGIC ## 7. Bronze Layer
# MAGIC
# MAGIC `01_bronze_ingestion` generates **326,308 deterministic baseline rows across 15 tables**. The generator is seeded for repeatability but anchored to the current Spark date so “last 30 days” and similar queries remain populated.
# MAGIC
# MAGIC ### Bronze tables and intent
# MAGIC
# MAGIC | Table | Approx. generated rows | Purpose |
# MAGIC |---|---:|---|
# MAGIC | `categories` | 8 | Product taxonomy |
# MAGIC | `suppliers` | 150 | Supplier master data |
# MAGIC | `stores` | 50 | Physical retail locations |
# MAGIC | `products` | 1,000 | Product catalog and economics |
# MAGIC | `customers` | 10,000 | Customer master and segmentation attributes |
# MAGIC | `orders` | 30,000 | Order headers and lifecycle status |
# MAGIC | `order_items` | 75,000 | Order line items and discounts |
# MAGIC | `payments` | 32,000 | Primary payments plus failed attempts |
# MAGIC | `shipments` | 28,000 | Shipment lifecycle data |
# MAGIC | `inventory_events` | 40,000 | Signed stock movements by product/warehouse |
# MAGIC | `returns` | 5,000 | Product-level returns and refunds |
# MAGIC | `clickstream_events` | 100,000 | Recent web/mobile behavioral activity |
# MAGIC | `support_tickets` | 3,000 | Customer service operations |
# MAGIC | `product_reviews` | 2,000 | Verified/unverified product feedback |
# MAGIC | `knowledge_documents` | 100 | Policies, runbooks, FAQs, and guides for the AI layer |
# MAGIC
# MAGIC ### Realism controls
# MAGIC
# MAGIC The generator deliberately enforces business relationships rather than creating independent random tables:
# MAGIC
# MAGIC - every order receives at least one line item;
# MAGIC - order header totals are reconciled from the line items;
# MAGIC - taxes are derived from discounted gross value;
# MAGIC - cancelled orders do not ship;
# MAGIC - non-cancelled orders have a completed primary payment;
# MAGIC - cancelled orders are represented by a refunded primary payment rather than recognized revenue;
# MAGIC - every order with status `Returned` is guaranteed to have an approved return;
# MAGIC - returned products must actually belong to the referenced order;
# MAGIC - refunds are capped at the purchased net value;
# MAGIC - inventory begins with opening stock and movements cannot make physical stock negative;
# MAGIC - verified reviews reference a customer/product combination from a completed or returned purchase;
# MAGIC - operational dates are bounded so future deliveries/returns are not fabricated.
# MAGIC
# MAGIC Every Bronze write also receives `_batch_id`, `_run_id`, and `_ingestion_ts` audit columns.
# MAGIC
# MAGIC ## 8. Silver Layer
# MAGIC
# MAGIC `02_silver_gold_pipeline` converts raw operational records into typed, consistent facts and dimensions.
# MAGIC
# MAGIC Key Silver objects include:
# MAGIC
# MAGIC - `dim_date`
# MAGIC - `dim_customers`
# MAGIC - `dim_products`
# MAGIC - `dim_stores`
# MAGIC - `dim_suppliers`
# MAGIC - `fact_orders`
# MAGIC - `fact_order_items`
# MAGIC - `fact_payments`
# MAGIC - `fact_shipments`
# MAGIC - `fact_returns`
# MAGIC - `fact_inventory_snapshot`
# MAGIC
# MAGIC Important business rules are centralized here. For example, `fact_orders.net_revenue` is zero for cancelled orders and subtracts approved refunds for recognized orders, never allowing the result to become negative. This prevents every downstream dashboard/model/agent from inventing its own revenue definition.
# MAGIC
# MAGIC ## 9. Gold Layer
# MAGIC
# MAGIC The Gold layer exposes business-ready data products:
# MAGIC
# MAGIC - `daily_sales_summary`
# MAGIC - `customer_kpis`
# MAGIC - `customer_lifetime_value`
# MAGIC - `customer_360`
# MAGIC - `product_kpis`
# MAGIC - `store_performance`
# MAGIC - `supplier_performance`
# MAGIC - `inventory_health`
# MAGIC - `support_operations`
# MAGIC - `executive_kpi`
# MAGIC - `data_quality_kpi`
# MAGIC - `anomaly_summary`
# MAGIC - `live_sales_minute` for the notebook streaming path
# MAGIC
# MAGIC ### Important correctness fix: product KPIs
# MAGIC
# MAGIC Sales, refunds, and reviews are aggregated **separately** before being joined by product. A direct join between order-item rows and review rows would create a many-to-many multiplication and inflate units/revenue. The project explicitly avoids that error.
# MAGIC
# MAGIC ### Order attempts vs recognized orders
# MAGIC
# MAGIC The project preserves both concepts:
# MAGIC
# MAGIC - **order attempts** include all orders;
# MAGIC - **recognized orders** exclude cancelled orders.
# MAGIC
# MAGIC This makes cancellation analysis possible without overstating completed business activity.
# MAGIC
# MAGIC ## 10. Data Quality Framework
# MAGIC
# MAGIC The current setup contains **36 enabled metadata-driven quality rules**.
# MAGIC
# MAGIC Rules are stored in `retail_quality.quality_rules`, including:
# MAGIC
# MAGIC - rule ID;
# MAGIC - dataset;
# MAGIC - field;
# MAGIC - rule type;
# MAGIC - business description;
# MAGIC - severity;
# MAGIC - acceptable threshold;
# MAGIC - SQL check expression.
# MAGIC
# MAGIC Rule families include uniqueness, completeness, validity, referential integrity, timeliness, and cross-table consistency.
# MAGIC
# MAGIC `03_data_quality_checks` reads the enabled rule metadata and executes every rule dynamically. It writes:
# MAGIC
# MAGIC - `quality_results` — latest execution;
# MAGIC - `quality_results_history` — append-only run history;
# MAGIC - `quality_summary` — severity-level rollup;
# MAGIC - `retail_gold.data_quality_kpi` — executive KPI view of quality health.
# MAGIC
# MAGIC The framework distinguishes a rule **FAIL** from an execution **ERROR**. A fail means the rule ran successfully and violations exceeded its threshold. An error means the quality engine itself could not execute the rule.
# MAGIC
# MAGIC ## 11. ML and MLOps
# MAGIC
# MAGIC `04_ml_training` demonstrates four analytical capabilities.
# MAGIC
# MAGIC ### Churn classification
# MAGIC
# MAGIC A scikit-learn Pipeline packages `StandardScaler` and `RandomForestClassifier` together. This is important because the logged MLflow model contains both preprocessing and prediction behavior rather than expecting a caller to reproduce scaling separately.
# MAGIC
# MAGIC The synthetic demonstration label treats explicitly churned customers, plus long-inactive customers, as positive churn cases. The model logs parameters, accuracy, ROC-AUC where calculable, and the complete fitted Pipeline.
# MAGIC
# MAGIC ### Customer segmentation
# MAGIC
# MAGIC A second Pipeline packages `StandardScaler` + `KMeans(k=4)`. Cluster numbers are **not** assumed to have fixed business meaning. The code profiles each cluster and orders them using a value score based on revenue, frequency, and recency, then maps them to `Low Value`, `Developing`, `High Value`, and `Champions`.
# MAGIC
# MAGIC ### Anomaly detection
# MAGIC
# MAGIC A third Pipeline packages `StandardScaler` + `IsolationForest`. The raw decision function is negated so higher stored values correspond more intuitively to more anomalous behavior. Customer-level anomaly output is stored for monitoring and agent use.
# MAGIC
# MAGIC ### Demand baseline
# MAGIC
# MAGIC `retail_ml.demand_forecast` is intentionally described as a **baseline**, not a calibrated forecasting model. It estimates next-30-day units from a 90-calendar-day average and produces a stability score based on active-day coverage and variability.
# MAGIC
# MAGIC ### MLflow
# MAGIC
# MAGIC The notebook uses a per-user experiment by default. The helper attempts the newer MLflow model `name=` API first and falls back to `artifact_path=` for compatibility with older runtimes.
# MAGIC
# MAGIC ## 12. Grounded GenAI / Agent System
# MAGIC
# MAGIC `05_genai_agent` implements a deterministic, tool-based assistant over lakehouse data.
# MAGIC
# MAGIC ### Knowledge grounding
# MAGIC
# MAGIC Active Bronze knowledge documents are normalized into `retail_genai.knowledge_documents`, then represented in `document_chunks`. Retrieval uses Spark column predicates rather than interpolating raw user input into SQL text.
# MAGIC
# MAGIC ### Tools
# MAGIC
# MAGIC Six tools are implemented:
# MAGIC
# MAGIC 1. read-only SQL tool;
# MAGIC 2. lexical retrieval tool;
# MAGIC 3. controlled metric lookup tool;
# MAGIC 4. anomaly/pipeline alert tool;
# MAGIC 5. platform health/status tool;
# MAGIC 6. platform summary report tool.
# MAGIC
# MAGIC The SQL tool only allows a single `SELECT`/`WITH` statement, rejects administrative/mutating keywords, and caps returned rows.
# MAGIC
# MAGIC ### Agents
# MAGIC
# MAGIC Five specialist agents are defined:
# MAGIC
# MAGIC - AnalyticsAgent
# MAGIC - KnowledgeAgent
# MAGIC - QualityAgent
# MAGIC - AnomalyAgent
# MAGIC - OperationsAgent
# MAGIC
# MAGIC The orchestrator routes queries by explicit word/phrase boundaries rather than fragile substring matching.
# MAGIC
# MAGIC ### Safety
# MAGIC
# MAGIC The safety filter rejects common prompt-injection instructions, credential requests, and destructive data requests before routing occurs.
# MAGIC
# MAGIC ### Evaluation
# MAGIC
# MAGIC A deterministic test set checks expected route, support/grounding status, safety rejection behavior, and latency. Results are written to:
# MAGIC
# MAGIC - `agent_interactions`
# MAGIC - `evaluation_results`
# MAGIC
# MAGIC The project presents those measurements as a **demo routing/source-support evaluation**, not as general LLM accuracy.
# MAGIC
# MAGIC ## 13. Free Edition Near-Real-Time Streaming
# MAGIC
# MAGIC `06_realtime_streaming` implements a serverless-safe incremental streaming pattern.
# MAGIC
# MAGIC ### What happens on each run
# MAGIC
# MAGIC 1. A finite batch of new JSON order events is written to the governed Unity Catalog volume.
# MAGIC 2. `spark.readStream` reads all unprocessed files using an explicit schema.
# MAGIC 3. `foreachBatch` deduplicates the micro-batch.
# MAGIC 4. Bronze uses Delta `MERGE` keyed by `event_id`.
# MAGIC 5. Silver uses Delta `MERGE` keyed by `order_id`.
# MAGIC 6. The affected `(event_minute, channel)` keys are identified.
# MAGIC 7. Gold recomputes those minute/channel aggregates from deduplicated Silver data.
# MAGIC 8. `streaming_metrics` is merged by `(stream_name, microbatch_id)`.
# MAGIC 9. `Trigger.AvailableNow()` processes all currently available files and exits.
# MAGIC 10. The checkpoint remembers processed files, so a later run only consumes new arrivals.
# MAGIC
# MAGIC This is **near-real-time incremental processing**, not hard real-time processing.
# MAGIC
# MAGIC ### Why Gold is recomputed instead of incremented
# MAGIC
# MAGIC Blindly adding a micro-batch total to an existing Gold total can double count when retries occur. The project instead identifies affected keys and recalculates those keys from Silver. That makes the aggregate substantially safer under retry/replay conditions.
# MAGIC
# MAGIC ## 14. Managed Lakeflow Realtime Path
# MAGIC
# MAGIC `06B_lakeflow_pipeline_source` is not intended for manual execution. The DAB registers it as the source of a serverless Lakeflow pipeline.
# MAGIC
# MAGIC It uses `pyspark.pipelines` decorators to define:
# MAGIC
# MAGIC - `orders_bronze_stream` — Auto Loader ingestion of the governed landing path;
# MAGIC - `orders_silver_stream` — typed/derived streaming order data;
# MAGIC - `live_sales_minute_pipeline` — a materialized minute/channel aggregate.
# MAGIC
# MAGIC The project therefore demonstrates both:
# MAGIC
# MAGIC - a bounded notebook/job pattern for Free Edition (`AvailableNow`); and
# MAGIC - a managed Lakeflow pipeline pattern for serverless streaming.
# MAGIC
# MAGIC ## 15. Observability
# MAGIC
# MAGIC Monitoring is treated as a first-class layer.
# MAGIC
# MAGIC `retail_monitoring.pipeline_runs` stores notebook/layer status, rows written, timestamps, and errors.
# MAGIC
# MAGIC `retail_monitoring.streaming_metrics` stores:
# MAGIC
# MAGIC - stream name;
# MAGIC - micro-batch ID;
# MAGIC - input/Bronze/Silver rows;
# MAGIC - start/end timestamps;
# MAGIC - processing milliseconds;
# MAGIC - approximate event lag;
# MAGIC - success/failure state;
# MAGIC - error message.
# MAGIC
# MAGIC The dashboard surfaces both business results and streaming health.
# MAGIC
# MAGIC ## 16. Runtime Validation
# MAGIC
# MAGIC `07_runtime_validation` is the final gate after notebooks 00–06.
# MAGIC
# MAGIC It verifies:
# MAGIC
# MAGIC - expected object existence;
# MAGIC - order/customer and item/order referential integrity;
# MAGIC - returned products belong to the order;
# MAGIC - cancelled orders are not shipped;
# MAGIC - no future orders are generated;
# MAGIC - order gross totals reconcile to line items;
# MAGIC - returned orders have approved returns;
# MAGIC - payment/order-state consistency;
# MAGIC - inventory does not finish negative;
# MAGIC - shipment and return dates are temporally valid;
# MAGIC - the quality engine did not produce execution errors;
# MAGIC - churn scoring covers the customer population;
# MAGIC - the agent routing evaluation has no mismatches;
# MAGIC - streaming output and successful micro-batches exist;
# MAGIC - the Unity Catalog volume exists;
# MAGIC - optional managed Lakeflow tables are validated when present.
# MAGIC
# MAGIC Results are written to `retail_monitoring.validation_results`. `PASS`, `FAIL`, and `SKIP` are deliberately distinct; optional Lakeflow checks are skipped when the pipeline has not been run.
# MAGIC
# MAGIC ## 17. Results Dashboard
# MAGIC
# MAGIC `RESULTS_DASHBOARD` provides notebook-native views for:
# MAGIC
# MAGIC - executive KPI;
# MAGIC - last-30-day revenue by channel;
# MAGIC - top Customer 360 records;
# MAGIC - churn risk distribution;
# MAGIC - customer segments;
# MAGIC - anomaly flags;
# MAGIC - demand baseline;
# MAGIC - inventory health;
# MAGIC - data-quality results;
# MAGIC - agent evaluation;
# MAGIC - agent interaction history;
# MAGIC - AvailableNow live sales;
# MAGIC - streaming health;
# MAGIC - optional managed Lakeflow live sales.
# MAGIC
# MAGIC The dashboard avoids hard-coded claims such as “100% accuracy” or a fixed table count. It queries measured results from the project tables.
# MAGIC
# MAGIC ## 18. Interactive Agent Chat
# MAGIC
# MAGIC `AGENT_CHAT` is the demo-facing assistant notebook. It provides a question widget, safety filter, route selection, governed queries, and formatted output. Typical questions include:
# MAGIC
# MAGIC - “What is the return policy?”
# MAGIC - “Show revenue for the last 14 days.”
# MAGIC - “Which customers are at high churn risk?”
# MAGIC - “What are the data quality results?”
# MAGIC - “Show pipeline status.”
# MAGIC - “Show live sales stream.”
# MAGIC - “Show me customer C0000042.”
# MAGIC
# MAGIC The live-sales route is checked before normal sales routing so a phrase such as “show live sales stream” reaches the realtime tool rather than the historical sales tool.
# MAGIC
# MAGIC ## 19. Declarative Automation Bundle Resource Graph
# MAGIC
# MAGIC The DAB's main workflow is intentionally sequential:
# MAGIC
# MAGIC ```text
# MAGIC setup
# MAGIC   -> bronze
# MAGIC   -> silver_gold
# MAGIC   -> data_quality
# MAGIC   -> ml_training
# MAGIC   -> genai
# MAGIC   -> realtime_available_now
# MAGIC   -> managed_pipeline_refresh
# MAGIC   -> runtime_validation
# MAGIC ```
# MAGIC
# MAGIC Other DAB resources:
# MAGIC
# MAGIC - `retail_ai_pipeline_refresh_job` — manually refreshes the managed realtime pipeline;
# MAGIC - `retail_ai_continuous_pipeline_job` — continuous job, deployed **PAUSED** to avoid unnecessary Free Edition quota use;
# MAGIC - `retail_ai_experiment` — MLflow experiment;
# MAGIC - Unity Catalog schemas + managed Volume.
# MAGIC
# MAGIC ## 20. Manual Run Order
# MAGIC
# MAGIC For a notebook-only demonstration:
# MAGIC
# MAGIC ```text
# MAGIC 00_START_HERE_FREE_EDITION
# MAGIC 00_setup
# MAGIC 01_bronze_ingestion
# MAGIC 02_silver_gold_pipeline
# MAGIC 03_data_quality_checks
# MAGIC 04_ml_training
# MAGIC 05_genai_agent
# MAGIC 06_realtime_streaming
# MAGIC 07_runtime_validation
# MAGIC RESULTS_DASHBOARD
# MAGIC AGENT_CHAT
# MAGIC ```
# MAGIC
# MAGIC `06_realtime_streaming` can be rerun to create/process another incremental batch.
# MAGIC
# MAGIC ## 21. DAB Deployment Flow
# MAGIC
# MAGIC From the extracted companion bundle directory:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate -t free
# MAGIC databricks bundle deploy -t free
# MAGIC databricks bundle run -t free retail_ai_e2e_job
# MAGIC ```
# MAGIC
# MAGIC If the target catalog is not `workspace`, override the `catalog` variable during deployment/run.
# MAGIC
# MAGIC ## 22. What Is Demonstration-Grade vs Production-Grade
# MAGIC
# MAGIC ### Strong production patterns already demonstrated
# MAGIC
# MAGIC - governed schemas and managed volume;
# MAGIC - Delta tables and idempotent merges;
# MAGIC - separated Bronze/Silver/Gold contracts;
# MAGIC - metadata-driven quality;
# MAGIC - audit metadata and operational telemetry;
# MAGIC - ML preprocessing packaged with models;
# MAGIC - MLflow experiment tracking;
# MAGIC - read-only and grounded AI tools;
# MAGIC - prompt-injection screening;
# MAGIC - streaming checkpointing;
# MAGIC - DAB infrastructure definitions;
# MAGIC - runtime validation.
# MAGIC
# MAGIC ### What a real production program would still add
# MAGIC
# MAGIC - real source connectors such as Kafka/Event Hubs/Kinesis/Auto Loader over actual object storage;
# MAGIC - secrets and service principals;
# MAGIC - environment-specific catalogs and permissions;
# MAGIC - CI/CD gates and automated bundle validation in source control;
# MAGIC - alert delivery to operational channels;
# MAGIC - model registry/approval/serving lifecycle;
# MAGIC - drift and feature monitoring;
# MAGIC - formal lineage/access policies;
# MAGIC - performance/load testing at real scale;
# MAGIC - SLA/SLO definitions and incident procedures.
# MAGIC
# MAGIC ## 23. Key Presentation Message
# MAGIC
# MAGIC The strongest story is not “this project has many notebooks.” The strongest story is:
# MAGIC
# MAGIC > **The same governed retail data moves from raw ingestion through trusted transformation, quality controls, ML, AI, streaming, observability, and automated deployment while keeping business definitions consistent.**
# MAGIC
# MAGIC That is the central architectural value of the project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## v4.2 Industry-Readiness Update
# MAGIC The Free Edition project now standardizes Spark timestamps to UTC, records duration/project version/environment in pipeline monitoring, blocks critical/error data-quality failures, captures DQ failure samples, and uses clearer semantic names including `fact_inventory_daily_movement`, `baseline_3yr_clv`, and `recency_risk`.
# MAGIC
# MAGIC ML training compares multiple churn classifiers, removes the direct recency leakage feature from churn training, evaluates K-Means cluster counts using silhouette score, and bases anomaly severity on anomaly score. The streaming path remains Free Edition-safe with `Trigger.AvailableNow()`, insert-only Bronze event handling, event-time ordering in Silver, and operational telemetry.
# MAGIC
# MAGIC Runtime validation now includes cross-layer reconciliation and blocking failures for core integrity checks. These are production-style patterns demonstrated within Databricks Free Edition constraints; real production deployment still requires real source systems, CI/CD, environment separation, permissions, secrets, SLOs, and real-data model validation.
