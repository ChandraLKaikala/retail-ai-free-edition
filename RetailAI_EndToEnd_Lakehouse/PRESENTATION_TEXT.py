# Databricks notebook source
# MAGIC %md
# MAGIC # RetailHub AI — Detailed Presentation Text
# MAGIC
# MAGIC ## Slide 1 — Real-Time Intelligent Retail Lakehouse
# MAGIC **On-slide text**
# MAGIC - Databricks RetailHub AI
# MAGIC - Batch + near-real-time data engineering
# MAGIC - Data quality, MLflow, GenAI agents, and live monitoring
# MAGIC
# MAGIC **Speaker notes**
# MAGIC This project demonstrates an end-to-end retail data and AI platform built on Databricks. The goal is not only to store retail data, but to move from raw operational events to trusted analytics, machine-learning predictions, grounded AI answers, and near-real-time business visibility within one lakehouse architecture.
# MAGIC
# MAGIC ## Slide 2 — The business problem
# MAGIC **On-slide text**
# MAGIC - Retail data is fragmented across orders, payments, inventory, stores, web activity, support, reviews, and policy content.
# MAGIC - Batch-only reporting creates stale operational views.
# MAGIC - Inconsistent transformations create conflicting KPIs.
# MAGIC - Ungrounded AI can produce answers disconnected from source data.
# MAGIC
# MAGIC **Speaker notes**
# MAGIC A retailer has both structured transactional data and unstructured operational knowledge. If each area is handled separately, the organization gets duplicated logic, delayed dashboards, weak traceability, and AI experiences that cannot reliably explain where an answer came from. The project solves that by creating common Delta-backed data contracts across engineering, analytics, ML, and AI.
# MAGIC
# MAGIC ## Slide 3 — End-to-end architecture
# MAGIC **On-slide text**
# MAGIC Sources → Bronze → Silver → Gold → ML / GenAI / Dashboard  
# MAGIC Quality + Monitoring across the platform  
# MAGIC Real-time orders → live Bronze → live Silver → minute-level Gold
# MAGIC
# MAGIC **Speaker notes**
# MAGIC Bronze stores source-like records with ingestion metadata. Silver standardizes types and creates trusted facts and dimensions. Gold publishes business-facing metrics such as customer 360, product performance, inventory health, and executive KPIs. MLflow tracks models and experiments. The agent layer reads governed tables through deterministic tools. Monitoring records both batch runs and streaming micro-batch health.
# MAGIC
# MAGIC ## Slide 4 — Bronze layer: realistic, fresh source data
# MAGIC **On-slide text**
# MAGIC - 15 baseline Delta tables
# MAGIC - 326,308 generated rows before streaming
# MAGIC - Current-run date anchor
# MAGIC - Audit metadata on every table
# MAGIC - Relational consistency across orders, items, shipments, and returns
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The original project already generated a broad retail dataset, but several relationships were synthetic in a way that could make downstream quality metrics misleading. The optimized generator guarantees every order has an item, reconciles order totals to line totals, keeps product prices tied to the product catalog, makes shipment and return dates follow order dates, and ensures returned products were actually purchased. Dates are anchored to the run date, so a “last 30 days” chart now shows recent information.
# MAGIC
# MAGIC ## Slide 5 — Silver layer: trusted data contracts
# MAGIC **On-slide text**
# MAGIC - Typed dates and standardized fields
# MAGIC - Customer, product, store, supplier dimensions
# MAGIC - Order, item, payment, shipment, return, inventory facts
# MAGIC - Consistent revenue calculation
# MAGIC
# MAGIC **Speaker notes**
# MAGIC Silver is the contract between raw ingestion and downstream consumers. Dates are converted to real DATE types instead of remaining strings. Customer emails are normalized, product margins are calculated consistently, cancelled orders contribute zero recognized revenue, and facts expose stable business fields. ML, dashboards, quality analysis, and agents can therefore consume the same trusted definitions.
# MAGIC
# MAGIC ## Slide 6 — Gold layer: business-ready analytics
# MAGIC **On-slide text**
# MAGIC - Daily sales summary
# MAGIC - Customer KPI / lifetime value / customer 360
# MAGIC - Product KPI
# MAGIC - Store and supplier performance
# MAGIC - Inventory health
# MAGIC - Executive KPI
# MAGIC
# MAGIC **Speaker notes**
# MAGIC Gold turns technical data into decision-oriented products. A critical correctness issue was fixed here: the original product KPI logic joined order items and reviews directly by product. Because both are one-to-many relationships, that can multiply sales rows by review rows and inflate product revenue. The optimized version first aggregates sales and reviews independently, then joins the two aggregates to the product dimension.
# MAGIC
# MAGIC ## Slide 7 — Metadata-driven data quality
# MAGIC **On-slide text**
# MAGIC - 36 active rules
# MAGIC - Uniqueness, completeness, validity
# MAGIC - Referential integrity, timeliness, consistency
# MAGIC - Severity + threshold model
# MAGIC - Latest results + historical run history
# MAGIC
# MAGIC **Speaker notes**
# MAGIC Instead of embedding every validation directly in transformation code, quality rules are stored as metadata with a dataset, field, rule type, SQL expression, severity, and failure threshold. The quality engine evaluates enabled rules dynamically. The current result table stays simple for dashboards while a new history table keeps every quality run for auditing and trend analysis.
# MAGIC
# MAGIC ## Slide 8 — ML and MLflow improvements
# MAGIC **On-slide text**
# MAGIC - Churn: Random Forest pipeline
# MAGIC - Segmentation: K-Means
# MAGIC - Anomaly detection: Isolation Forest
# MAGIC - Demand baseline from recent sales
# MAGIC - MLflow experiment tracking
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The most important ML change is reproducibility. Previously the churn model was logged separately from the StandardScaler, which creates a serving risk because a loaded classifier does not know how inputs were scaled. The optimized version logs an sklearn Pipeline containing both preprocessing and the classifier. K-Means labels are assigned based on each cluster’s observed revenue, order frequency, and recency instead of treating numeric cluster IDs as stable business meaning. The previous random demand numbers are replaced with a deterministic 90-day calendar-day historical baseline.
# MAGIC
# MAGIC ## Slide 9 — Grounded GenAI and agent tools
# MAGIC **On-slide text**
# MAGIC - Read-only SQL tool
# MAGIC - Knowledge retrieval tool
# MAGIC - KPI tool
# MAGIC - Data-quality tool
# MAGIC - Anomaly tool
# MAGIC - Operations/status tool
# MAGIC - Safety filter + interaction logging
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The agent is designed around tools rather than free-form claims. It retrieves policy text from a curated knowledge table and reads metrics from Silver, Gold, ML, Quality, and Monitoring tables. The SQL tool only accepts a single SELECT or WITH statement. Retrieval uses Spark expressions rather than injecting raw user text into SQL. Tool routing uses word/phrase-aware matching instead of brittle substring checks; response status and latency are stored for evaluation.
# MAGIC
# MAGIC ## Slide 10 — Why the original project did not yet look real-time
# MAGIC **On-slide text**
# MAGIC - Bronze ingestion was batch generation.
# MAGIC - Silver/Gold tables were rebuilt with CREATE OR REPLACE.
# MAGIC - Dashboard freshness depended on old synthetic dates.
# MAGIC - No streaming checkpoint, micro-batch, or stream-latency metrics existed.
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The project documentation correctly identified streaming as a future production step, but the executable pipeline itself was still batch-oriented. That means it could describe a real-time platform without visibly behaving like one. The optimization adds an explicit streaming notebook and live dashboard surfaces so the demo and the architecture statement now agree.
# MAGIC
# MAGIC ## Slide 11 — Real-time streaming pipeline
# MAGIC **On-slide text**
# MAGIC Unity Catalog landing files → `Trigger.AvailableNow()` → `foreachBatch` → Bronze MERGE → Silver MERGE → Gold minute KPI MERGE
# MAGIC
# MAGIC **Speaker notes**
# MAGIC For the Free Edition notebook demo, each run lands a finite batch of realistic order-like JSON events in a governed Unity Catalog Volume. Structured Streaming then processes all newly available files with `Trigger.AvailableNow()` and exits cleanly. The companion DAB also deploys a managed Lakeflow pipeline using Auto Loader for the same landing path; that is the preferred always-on serverless pattern.
# MAGIC
# MAGIC ## Slide 12 — Checkpointing and retry-safe processing
# MAGIC **On-slide text**
# MAGIC - Stream checkpoint tracks progress.
# MAGIC - Bronze deduplicates by `event_id`.
# MAGIC - Silver upserts by `order_id`.
# MAGIC - Gold recomputes only touched minute/channel keys.
# MAGIC - Avoids additive double counting on retries.
# MAGIC
# MAGIC **Speaker notes**
# MAGIC A convincing real-time design must consider retries. Simply appending every `foreachBatch` result can duplicate data if a batch is retried after a partial failure. The optimized path merges Bronze by event ID and Silver by order ID. Instead of incrementing Gold totals, it recalculates the affected minute/channel aggregates from the deduplicated Silver table and merges those values into Gold. This is a much safer idempotent pattern for a demo pipeline.
# MAGIC
# MAGIC ## Slide 13 — Live operational observability
# MAGIC **On-slide text**
# MAGIC Streaming metrics capture:
# MAGIC - micro-batch ID
# MAGIC - input rows
# MAGIC - Bronze/Silver rows
# MAGIC - processing milliseconds
# MAGIC - status
# MAGIC - completion timestamp
# MAGIC
# MAGIC **Speaker notes**
# MAGIC Every successful or failed micro-batch writes an operational record to `retail_monitoring.streaming_metrics`. This makes the stream observable from the same lakehouse. The dashboard shows changing business KPIs and the technical behavior of each incremental streaming run, including processing time and event lag.
# MAGIC
# MAGIC ## Slide 14 — Dashboard experience
# MAGIC **On-slide text**
# MAGIC - Executive KPIs
# MAGIC - Last-30-day revenue
# MAGIC - Customer 360
# MAGIC - ML risk and segments
# MAGIC - Anomalies and demand
# MAGIC - Inventory health
# MAGIC - Data quality
# MAGIC - Agent evaluation
# MAGIC - Live sales + streaming health
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The dashboard was cleaned so it references outputs that are actually produced by the code. Unsupported claims such as a fixed 78-table count and blanket “100% accuracy” were removed. Agent routing accuracy is measured on a defined evaluation set, while streaming sections display the newest minute aggregates and micro-batch metrics after each AvailableNow increment or managed Lakeflow refresh.
# MAGIC
# MAGIC ## Slide 15 — Interactive AI demo
# MAGIC **On-slide text**
# MAGIC Example questions:
# MAGIC - “What is the return policy?”
# MAGIC - “Show revenue for the last 14 days.”
# MAGIC - “Which customers are at high churn risk?”
# MAGIC - “Show data quality results.”
# MAGIC - “Show live sales stream.”
# MAGIC
# MAGIC **Speaker notes**
# MAGIC AGENT_CHAT provides a simple notebook interface for demonstrating the tool layer. Policy questions query the knowledge base, sales questions query Gold, churn questions use ML results, quality questions use the metadata-driven framework, and the new real-time route reads the live Gold stream output.
# MAGIC
# MAGIC ## Slide 16 — How to demonstrate the platform live
# MAGIC **On-slide text**
# MAGIC 1. Run notebooks 00–05.
# MAGIC 2. Run `06_realtime_streaming` to land and process a new AvailableNow increment.
# MAGIC 3. Optionally refresh the DAB-managed Lakeflow pipeline.
# MAGIC 4. Run `07_runtime_validation` and confirm no FAIL checks.
# MAGIC 5. Open `RESULTS_DASHBOARD`, then ask AGENT_CHAT: “Show live sales stream.”
# MAGIC
# MAGIC **Speaker notes**
# MAGIC This sequence starts with stable historical context and then adds a new incremental event batch using a Free Edition-supported streaming trigger. Re-running the streaming notebook produces another increment, while the optional Lakeflow pipeline demonstrates the managed serverless path. The agent and dashboard read the same governed lakehouse outputs.
# MAGIC
# MAGIC ## Slide 17 — Production hardening roadmap
# MAGIC **On-slide text**
# MAGIC - Replace demo landing-file generation with the real enterprise event source.
# MAGIC - Keep Declarative Automation Bundles + CI/CD + Lakeflow Jobs as the deployment layer.
# MAGIC - Environment-specific catalogs/checkpoints.
# MAGIC - Unity Catalog least privilege and secrets.
# MAGIC - Streaming SLOs and alerts.
# MAGIC - Model Registry, serving, and drift monitoring.
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The optimized archive is a credible real-time demonstration, not a claim that every production control is complete. The next step is operationalization: connect a real event bus, deploy repeatably, establish access control, define freshness and latency objectives, route alerts, and add model serving/drift monitoring if online predictions are required.
# MAGIC
# MAGIC ## Slide 18 — Final outcome
# MAGIC **On-slide text**
# MAGIC One platform with consistent data contracts, measurable quality, reproducible ML, grounded AI, and near-real-time business visibility.
# MAGIC
# MAGIC **Speaker notes**
# MAGIC The key optimization is alignment. The code, dashboard, agent tools, and presentation now describe the same architecture. The project retains its broad end-to-end scope but is more technically credible because the data relationships are consistent, the KPI logic is correct, the ML artifacts are reproducible, the agent queries real schemas, and the platform visibly processes streaming events.

# COMMAND ----------

# MAGIC %md
# MAGIC ## v4.2 Industry-Readiness Update
# MAGIC The Free Edition project now standardizes Spark timestamps to UTC, records duration/project version/environment in pipeline monitoring, blocks critical/error data-quality failures, captures DQ failure samples, and uses clearer semantic names including `fact_inventory_daily_movement`, `baseline_3yr_clv`, and `recency_risk`.
# MAGIC
# MAGIC ML training compares multiple churn classifiers, removes the direct recency leakage feature from churn training, evaluates K-Means cluster counts using silhouette score, and bases anomaly severity on anomaly score. The streaming path remains Free Edition-safe with `Trigger.AvailableNow()`, insert-only Bronze event handling, event-time ordering in Silver, and operational telemetry.
# MAGIC
# MAGIC Runtime validation now includes cross-layer reconciliation and blocking failures for core integrity checks. These are production-style patterns demonstrated within Databricks Free Edition constraints; real production deployment still requires real source systems, CI/CD, environment separation, permissions, secrets, SLOs, and real-data model validation.
