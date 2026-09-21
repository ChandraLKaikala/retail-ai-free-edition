# Databricks notebook source
# MAGIC %md
# MAGIC # RetailHub AI — Detailed Presentation: What, Why, and How
# MAGIC
# MAGIC This script is designed for a technical presentation, viva, portfolio review, or interview. Each slide has four layers: **On-slide text**, **What**, **Why**, **How**, plus **Speaker notes**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 1 — RetailHub AI: End-to-End Retail Intelligence on Databricks
# MAGIC
# MAGIC ### On-slide text
# MAGIC - Databricks Free Edition E2E v4.0
# MAGIC - Lakehouse + Quality + MLflow + GenAI + Streaming + DABs
# MAGIC - One governed data foundation from raw data to decision support
# MAGIC
# MAGIC ### What
# MAGIC RetailHub AI is a complete retail lakehouse demonstration built as a set of Databricks notebooks plus a Declarative Automation Bundle.
# MAGIC
# MAGIC ### Why
# MAGIC The goal is to demonstrate more than ETL. A realistic modern data platform must also show data trust, ML lifecycle, AI grounding, operational monitoring, incremental processing, and repeatable deployment.
# MAGIC
# MAGIC ### How
# MAGIC The project moves data through Bronze, Silver, and Gold; executes metadata-driven quality; trains MLflow-tracked models; exposes governed agent tools; processes incremental events with Structured Streaming; deploys a managed Lakeflow pipeline through DABs; and validates the final runtime state.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The main point is integration. Each capability uses the same governed data contracts instead of being a disconnected demo. Revenue used by the dashboard, ML features, and agent all originates from the same Silver/Gold logic.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 2 — The Retail Data Problem
# MAGIC
# MAGIC ### On-slide text
# MAGIC Customer + Orders + Products + Payments + Inventory + Returns + Support + Web + Documents
# MAGIC
# MAGIC **Problem:** fragmented systems produce inconsistent metrics and slow decisions.
# MAGIC
# MAGIC ### What
# MAGIC Retail data exists across multiple operational domains with different grains, update patterns, and business rules.
# MAGIC
# MAGIC ### Why
# MAGIC If each team independently defines revenue, active customers, returns, inventory health, or churn features, the organization gets contradictory answers.
# MAGIC
# MAGIC ### How
# MAGIC The project centralizes those domains in a governed lakehouse and promotes data through explicit layers before analytics, ML, or AI consume it.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Use revenue as an example: a cancelled order should not count as recognized revenue, and an approved refund should reduce revenue. If that rule is not centralized, every dashboard or model can disagree.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 3 — Why Databricks Free Edition Changes the Design
# MAGIC
# MAGIC ### On-slide text
# MAGIC - Serverless-first
# MAGIC - Unity Catalog tables + managed Volume
# MAGIC - No DBFS-root dependency
# MAGIC - AvailableNow for notebook/job streaming
# MAGIC - Lakeflow for managed pipeline streaming
# MAGIC - Quota-aware orchestration
# MAGIC
# MAGIC ### What
# MAGIC The project is intentionally adapted to the capabilities and limits of Free Edition.
# MAGIC
# MAGIC ### Why
# MAGIC A project that requires classic clusters, DBFS-root paths, or unsupported streaming triggers is not truly Free Edition ready even if its Python syntax is valid.
# MAGIC
# MAGIC ### How
# MAGIC The notebooks use the current/writable catalog, Unity Catalog schemas, a managed Volume for files/checkpoints, and `Trigger.AvailableNow()` for bounded serverless streaming. The DAB adds a serverless Lakeflow pipeline for the managed path.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This is why the realtime design has two modes. AvailableNow is easy to run manually or as a Job. The Lakeflow pipeline demonstrates how the same concept becomes a managed serverless pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 4 — DBC vs DAB
# MAGIC
# MAGIC ### On-slide text
# MAGIC **DBC:** notebooks and documentation  
# MAGIC **DAB:** deployable Jobs, pipeline, schemas, Volume, MLflow experiment
# MAGIC
# MAGIC ### What
# MAGIC The deliverable uses two Databricks packaging concepts.
# MAGIC
# MAGIC ### Why
# MAGIC A notebook archive cannot automatically create the complete orchestration/resource layer.
# MAGIC
# MAGIC ### How
# MAGIC The DBC gives an easy importable workspace project. The DAB contains YAML resource definitions and source notebooks so `databricks bundle deploy` can create/update the deployment layer.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC If someone asks whether importing the DBC creates the pipeline, the answer is no. The bundle is what turns project resources into repeatable infrastructure-as-code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 5 — End-to-End Architecture
# MAGIC
# MAGIC ### On-slide text
# MAGIC Sources → Bronze → Silver → Gold → Dashboard / ML / AI
# MAGIC
# MAGIC Cross-cutting: Quality + Monitoring + Validation
# MAGIC
# MAGIC Realtime: Volume → Streaming → Bronze RT → Silver RT → Gold minute KPI
# MAGIC
# MAGIC ### What
# MAGIC This is the logical architecture of the project.
# MAGIC
# MAGIC ### Why
# MAGIC Layer separation makes raw history auditable, business logic reusable, and consumer-facing tables stable.
# MAGIC
# MAGIC ### How
# MAGIC Bronze retains operational records, Silver standardizes types/business rules, Gold publishes business products, and parallel ML/AI/streaming layers read those governed outputs.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Emphasize that streaming does not replace the batch history. The project has a historical baseline plus incremental minute-level metrics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 6 — Setup and Governance
# MAGIC
# MAGIC ### On-slide text
# MAGIC 9 schemas + managed Volume + quality metadata + monitoring tables
# MAGIC
# MAGIC ### What
# MAGIC `00_setup` initializes the governed namespace and project metadata.
# MAGIC
# MAGIC ### Why
# MAGIC Downstream notebooks should not each invent their own database names, monitoring tables, or storage locations.
# MAGIC
# MAGIC ### How
# MAGIC A catalog widget resolves the writable target catalog; identifier validation is applied; schemas are created; quality rules are loaded; monitoring tables and a managed Volume are initialized.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The key design principle is parameterization. The same code can run in another writable catalog without editing every SQL statement.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 7 — Bronze: 15 Operational Domains
# MAGIC
# MAGIC ### On-slide text
# MAGIC 326,308 deterministic baseline rows
# MAGIC
# MAGIC Customers, orders, items, products, payments, shipments, inventory, returns, clickstream, support, reviews, documents…
# MAGIC
# MAGIC ### What
# MAGIC The Bronze notebook builds a realistic synthetic retail source system.
# MAGIC
# MAGIC ### Why
# MAGIC An end-to-end demo requires enough cross-domain data to support analytics, ML, AI, and operational validation.
# MAGIC
# MAGIC ### How
# MAGIC Each table is deterministically generated from a fixed seed but anchored to the current date. Every write gets run/batch/ingestion audit metadata.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Deterministic does not mean static dates. The random sequence is repeatable, but dates are shifted relative to the execution date so recent dashboards remain useful.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 8 — Synthetic Data That Behaves Like a Real System
# MAGIC
# MAGIC ### On-slide text
# MAGIC - Every order has items
# MAGIC - Header totals reconcile to lines
# MAGIC - Cancelled orders do not ship
# MAGIC - Returned orders have approved returns
# MAGIC - Verified reviews map to purchases
# MAGIC - Inventory cannot become physically negative
# MAGIC
# MAGIC ### What
# MAGIC The generator encodes relationships between source tables.
# MAGIC
# MAGIC ### Why
# MAGIC Independent random tables often look impressive in row count but fail basic business questions.
# MAGIC
# MAGIC ### How
# MAGIC The notebook builds lookup structures while generating data, uses Delta MERGE to reconcile order headers, and explicitly constrains payment/shipment/return/review/inventory logic.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This is one of the most important improvements. A reviewer can test cross-table relationships and get coherent answers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 9 — Silver: Trusted Facts and Dimensions
# MAGIC
# MAGIC ### On-slide text
# MAGIC Typed dimensions + consistent facts + centralized business semantics
# MAGIC
# MAGIC ### What
# MAGIC Silver converts raw operational data into analytics-ready contracts.
# MAGIC
# MAGIC ### Why
# MAGIC Consumers should not repeatedly parse dates, normalize email, calculate margins, or interpret refund/order status logic.
# MAGIC
# MAGIC ### How
# MAGIC The notebook creates reusable dimensions and facts, including customer/product/store/supplier dimensions and order/item/payment/shipment/return/inventory facts.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Silver is where raw “source truth” becomes “trusted analytical truth.”
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 10 — Revenue Correctness
# MAGIC
# MAGIC ### On-slide text
# MAGIC Recognized revenue = discounted sales − approved refunds
# MAGIC
# MAGIC Cancelled orders = 0 recognized revenue
# MAGIC
# MAGIC ### What
# MAGIC The order fact has an explicit `net_revenue` definition.
# MAGIC
# MAGIC ### Why
# MAGIC Revenue is the most visible KPI and the easiest place for synthetic/demo projects to become logically wrong.
# MAGIC
# MAGIC ### How
# MAGIC Approved refunds are aggregated by order, joined to orders, and subtracted from discounted sales. The value is floored at zero, and cancelled orders are forced to zero recognized revenue.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Tax remains available as a separate operational field. The project's recognized-revenue metric focuses on merchandise sales after discount/refund.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 11 — Gold: Business Data Products
# MAGIC
# MAGIC ### On-slide text
# MAGIC Daily Sales | Customer 360 | Product KPI | Store | Supplier | Inventory | Support | Executive KPI
# MAGIC
# MAGIC ### What
# MAGIC Gold contains consumer-oriented, pre-aggregated data products.
# MAGIC
# MAGIC ### Why
# MAGIC Dashboards, agents, and ML should read stable business objects rather than rebuild complex joins on every request.
# MAGIC
# MAGIC ### How
# MAGIC Silver facts/dimensions are aggregated into subject-area marts with business-friendly columns and clear grains.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The project distinguishes “order attempts” from “recognized orders,” which preserves cancellation information without inflating business activity.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 12 — Fixing the Product KPI Many-to-Many Problem
# MAGIC
# MAGIC ### On-slide text
# MAGIC Sales aggregate  +  Refund aggregate  +  Review aggregate  → Product KPI
# MAGIC
# MAGIC ### What
# MAGIC Product sales, refunds, and reviews are aggregated independently before joining.
# MAGIC
# MAGIC ### Why
# MAGIC Joining raw item rows directly to raw review rows by product multiplies rows and inflates revenue/units.
# MAGIC
# MAGIC ### How
# MAGIC Three CTEs produce one row per product for sales/refunds/reviews; those aggregates are then left-joined to the product dimension.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This is a concrete example of data engineering correctness—not just optimization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 13 — Metadata-Driven Data Quality
# MAGIC
# MAGIC ### On-slide text
# MAGIC 36 enabled rules
# MAGIC
# MAGIC Uniqueness | Completeness | Validity | Referential Integrity | Timeliness | Consistency
# MAGIC
# MAGIC ### What
# MAGIC Quality rules are stored as metadata rather than procedural code branches.
# MAGIC
# MAGIC ### Why
# MAGIC The framework should scale when a new rule is added without changing the execution engine.
# MAGIC
# MAGIC ### How
# MAGIC Each rule stores a SQL check expression, severity, threshold, dataset, and description. The quality notebook loops over enabled rules, executes them, calculates failure percentages, and writes latest + history results.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC PASS means violations are within threshold. FAIL means the rule executed but exceeded threshold. ERROR means the rule itself could not execute.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 14 — Data Quality Examples
# MAGIC
# MAGIC ### On-slide text
# MAGIC - Order/customer FK
# MAGIC - Order total vs item total
# MAGIC - Tax reconciliation
# MAGIC - Returned product belongs to order
# MAGIC - Cancelled order not shipped
# MAGIC - Verified review has real purchase
# MAGIC - Returned order has approved return
# MAGIC
# MAGIC ### What
# MAGIC The quality rules validate cross-table business invariants, not only null checks.
# MAGIC
# MAGIC ### Why
# MAGIC Cross-domain contradictions are what make synthetic or operational datasets untrustworthy.
# MAGIC
# MAGIC ### How
# MAGIC SQL `NOT EXISTS`, joins, aggregate reconciliation, status-domain checks, and temporal conditions encode the invariants.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Mention that signed inventory quantities are valid; a sale being negative is not automatically a quality error.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 15 — ML/MLOps Architecture
# MAGIC
# MAGIC ### On-slide text
# MAGIC Gold features → sklearn Pipeline → MLflow → Delta prediction tables → Dashboard/Agent
# MAGIC
# MAGIC ### What
# MAGIC The project includes churn classification, customer segmentation, anomaly detection, and a demand baseline.
# MAGIC
# MAGIC ### Why
# MAGIC The ML layer demonstrates the complete path from governed features to tracked models to consumable outputs.
# MAGIC
# MAGIC ### How
# MAGIC The notebook reads Gold/Silver features into pandas, fits scikit-learn Pipelines, logs metrics/models to MLflow, and writes predictions/segments/anomalies back to Delta.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The project is intentionally small enough for Free Edition but preserves production patterns such as preprocessing packaged with the model.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 16 — Churn Model
# MAGIC
# MAGIC ### On-slide text
# MAGIC StandardScaler + RandomForestClassifier
# MAGIC
# MAGIC Metrics: Accuracy + ROC-AUC
# MAGIC
# MAGIC ### What
# MAGIC A binary classifier predicts a synthetic churn label.
# MAGIC
# MAGIC ### Why
# MAGIC Churn is a familiar business use case and reuses Customer 360 metrics such as recency, frequency, value, tenure, and loyalty.
# MAGIC
# MAGIC ### How
# MAGIC The model uses a stratified train/test split where possible, balanced class weights, logged hyperparameters, and full-population probability scoring.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Do not claim the metric proves real-world churn performance. The labels and data are synthetic; the value is the reproducible MLOps pattern.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 17 — Segmentation and Anomaly Detection
# MAGIC
# MAGIC ### On-slide text
# MAGIC K-Means k=4 | Isolation Forest
# MAGIC
# MAGIC ### What
# MAGIC Unsupervised models identify customer groups and unusual customers.
# MAGIC
# MAGIC ### Why
# MAGIC Not every retail problem has labels. Segmentation and anomaly detection demonstrate complementary ML modes.
# MAGIC
# MAGIC ### How
# MAGIC Each model has scaling inside the sklearn Pipeline. Cluster labels are derived after profiling instead of assuming cluster number meaning. Isolation Forest scores are stored with an intuitive anomaly direction.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC If asked why scale K-Means: distance-based clustering is highly sensitive to feature scale.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 18 — Demand Baseline
# MAGIC
# MAGIC ### On-slide text
# MAGIC 90-day calendar average → next-30-day baseline
# MAGIC
# MAGIC ### What
# MAGIC The project estimates product units for the next 30 days using recent historical daily volume.
# MAGIC
# MAGIC ### Why
# MAGIC A deterministic baseline is more honest and reproducible than a random “forecast.”
# MAGIC
# MAGIC ### How
# MAGIC It sums 90-day units, divides by 90 calendar days, scales to 30 days, and adds a stability score based on active-day coverage and variability.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Use the word “baseline,” not “production forecast.” It creates a benchmark that a future advanced forecasting model should beat.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 19 — Grounded GenAI Architecture
# MAGIC
# MAGIC ### On-slide text
# MAGIC Safety → Orchestrator → Specialist Agent → Governed Tool → Lakehouse
# MAGIC
# MAGIC ### What
# MAGIC The AI layer is a deterministic tool-using assistant grounded in project tables/documents.
# MAGIC
# MAGIC ### Why
# MAGIC A business agent should retrieve trusted facts instead of fabricating KPI values or executing unrestricted SQL.
# MAGIC
# MAGIC ### How
# MAGIC The system indexes enterprise documents, defines read-only tools, routes questions to five specialist agents, and logs every evaluation interaction.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This design demonstrates agent architecture without requiring an external paid model endpoint for the core demo.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 20 — AI Safety and SQL Guardrails
# MAGIC
# MAGIC ### On-slide text
# MAGIC - Prompt-injection pattern checks
# MAGIC - Sensitive request checks
# MAGIC - Destructive request checks
# MAGIC - SELECT/WITH-only SQL
# MAGIC - Mutating keyword rejection
# MAGIC - Row limit
# MAGIC
# MAGIC ### What
# MAGIC The project applies safety before tool execution and constrains the SQL surface.
# MAGIC
# MAGIC ### Why
# MAGIC Giving an agent unrestricted SQL is dangerous and unnecessary for an analytics assistant.
# MAGIC
# MAGIC ### How
# MAGIC The safety filter rejects suspicious prompt patterns; the SQL tool only permits a single read-only statement and separately rejects mutation/admin keywords.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This is a defense-in-depth demo, not a claim of perfect adversarial security.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 21 — Agent Evaluation
# MAGIC
# MAGIC ### On-slide text
# MAGIC Expected route | Actual route | Supported response | Latency
# MAGIC
# MAGIC ### What
# MAGIC A deterministic test set measures the routing/safety behavior.
# MAGIC
# MAGIC ### Why
# MAGIC “Agent works” should be supported by evidence rather than a few cherry-picked prompts.
# MAGIC
# MAGIC ### How
# MAGIC Ten test cases are executed and written to interaction/evaluation tables. The dashboard aggregates routing correctness and support status.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The evaluation is deliberately described as a small demo set. It should not be presented as generalized AI accuracy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 22 — Free Edition Near-Real-Time Path
# MAGIC
# MAGIC ### On-slide text
# MAGIC Volume files → readStream → foreachBatch → Bronze MERGE → Silver MERGE → Gold minute KPI
# MAGIC
# MAGIC ### What
# MAGIC `06_realtime_streaming` processes newly landed order-event files incrementally.
# MAGIC
# MAGIC ### Why
# MAGIC The project needs an executable realtime-like path that actually works with serverless Free Edition constraints.
# MAGIC
# MAGIC ### How
# MAGIC Each run lands a finite event batch, reads only files not already tracked by the checkpoint, processes them through Delta merges, updates minute/channel KPIs, writes metrics, and exits with `Trigger.AvailableNow()`.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Calling it near-real-time is precise. Rerun the notebook to simulate the next increment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 23 — Idempotency and Retry Safety
# MAGIC
# MAGIC ### On-slide text
# MAGIC Bronze key: `event_id`  
# MAGIC Silver key: `order_id`  
# MAGIC Gold: recalculate affected minute/channel keys  
# MAGIC Metrics key: stream + microbatch
# MAGIC
# MAGIC ### What
# MAGIC The streaming path is designed to avoid naive duplicate accumulation.
# MAGIC
# MAGIC ### Why
# MAGIC Streaming failures/retries can process the same logical data more than once if writes are simple appends/increments.
# MAGIC
# MAGIC ### How
# MAGIC Delta MERGE upserts deterministic keys. Gold aggregates are recomputed for the touched keys from deduplicated Silver state, and metrics are also upserted.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Exactly-once behavior is a system property, not just one line of code, but this architecture demonstrates the correct direction.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 24 — Managed Lakeflow Pipeline
# MAGIC
# MAGIC ### On-slide text
# MAGIC Auto Loader Bronze → Silver streaming table → minute materialized view
# MAGIC
# MAGIC ### What
# MAGIC The companion DAB deploys `06B_lakeflow_pipeline_source` as a serverless Lakeflow pipeline.
# MAGIC
# MAGIC ### Why
# MAGIC A managed pipeline is the cleaner serverless approach when the source should be continuously processed and orchestrated as a pipeline resource.
# MAGIC
# MAGIC ### How
# MAGIC `pyspark.pipelines` decorators declare streaming tables and a materialized KPI view. The source catalog is supplied through pipeline configuration.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The DBC contains the source notebook, but the DAB is what creates the actual pipeline resource.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 25 — Observability
# MAGIC
# MAGIC ### On-slide text
# MAGIC Pipeline runs + Streaming metrics + Quality history + Validation results + Agent logs
# MAGIC
# MAGIC ### What
# MAGIC The project records technical health alongside business outputs.
# MAGIC
# MAGIC ### Why
# MAGIC A system that produces a KPI without telling you whether the pipeline succeeded or data is fresh is difficult to trust.
# MAGIC
# MAGIC ### How
# MAGIC Batch notebooks append to `pipeline_runs`; streaming records rows/time/lag/status; quality keeps latest/history; validation stores PASS/FAIL/SKIP; agent interactions are logged.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This lets you answer both “what is revenue?” and “how do I know the data pipeline that produced it succeeded?”
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 26 — Runtime Validation as the Final Gate
# MAGIC
# MAGIC ### On-slide text
# MAGIC Objects | FKs | Financials | Dates | Payments | Inventory | Quality | ML | AI | Streaming | Volume | Lakeflow
# MAGIC
# MAGIC ### What
# MAGIC `07_runtime_validation` is an acceptance-test notebook.
# MAGIC
# MAGIC ### Why
# MAGIC Static syntax validation cannot prove Spark SQL semantics, workspace permissions, Delta MERGE behavior, or streaming runtime behavior.
# MAGIC
# MAGIC ### How
# MAGIC The notebook executes cross-table assertions in the real workspace, persists results, and clearly distinguishes required failures from optional skipped Lakeflow checks.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This is the most defensible way to talk about verification: static checks happen before import; runtime checks happen inside Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 27 — Results Dashboard
# MAGIC
# MAGIC ### On-slide text
# MAGIC Executive KPI | Revenue | Customer 360 | Churn | Segments | Anomalies | Demand | Inventory | Quality | AI | Streaming
# MAGIC
# MAGIC ### What
# MAGIC A notebook-native dashboard brings the project outputs together.
# MAGIC
# MAGIC ### Why
# MAGIC A reviewer should be able to see the business impact without manually querying dozens of tables.
# MAGIC
# MAGIC ### How
# MAGIC Each section reads a governed Gold/ML/quality/agent/monitoring table and uses `display()` for interactive visualization.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The dashboard does not hard-code “100% accuracy” or a fixed table count; it surfaces measured tables/results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 28 — Declarative Automation Bundle
# MAGIC
# MAGIC ### On-slide text
# MAGIC `databricks.yml` + resource YAML + source notebooks + tests
# MAGIC
# MAGIC ### What
# MAGIC The DAB turns the project into a repeatable deployment unit.
# MAGIC
# MAGIC ### Why
# MAGIC Manual task wiring is difficult to reproduce and easy to drift from source code.
# MAGIC
# MAGIC ### How
# MAGIC Resource YAML defines schemas, Volume, MLflow experiment, main sequential Job, realtime pipeline, refresh Job, and paused continuous Job.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC This is the bridge from “notebook project” to “deployable Databricks project.”
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 29 — Main DAB Workflow
# MAGIC
# MAGIC ### On-slide text
# MAGIC setup → bronze → silver/gold → quality → ML → GenAI → AvailableNow → Lakeflow refresh → validation
# MAGIC
# MAGIC ### What
# MAGIC The bundle defines one ordered end-to-end Job.
# MAGIC
# MAGIC ### Why
# MAGIC Sequential execution keeps dependencies explicit and is conservative with Free Edition concurrency/quota.
# MAGIC
# MAGIC ### How
# MAGIC Each task receives the catalog parameter. ML additionally receives the bundle-managed MLflow experiment. The pipeline task refreshes the managed realtime pipeline before final validation.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The optional continuous pipeline job is separate and paused by default.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 30 — Live Demo Sequence
# MAGIC
# MAGIC ### On-slide text
# MAGIC 1. Run/deploy baseline
# MAGIC 2. Show Executive KPI
# MAGIC 3. Run AvailableNow increment
# MAGIC 4. Refresh realtime dashboard
# MAGIC 5. Show streaming metrics
# MAGIC 6. Ask agent “Show live sales stream”
# MAGIC 7. Show runtime validation
# MAGIC
# MAGIC ### What
# MAGIC This is the recommended presentation flow.
# MAGIC
# MAGIC ### Why
# MAGIC It proves multiple layers are connected instead of showing isolated code screenshots.
# MAGIC
# MAGIC ### How
# MAGIC Start with stable historical results, introduce new events, show that Gold and telemetry change, then query the same output through the agent and finish with validation evidence.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC The story becomes: data arrived, engineering processed it, monitoring observed it, business metrics changed, the agent read the same governed result, and validation confirmed consistency.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 31 — What I Would Add for Production
# MAGIC
# MAGIC ### On-slide text
# MAGIC Real event bus | CI/CD | Service principals | Secrets | SLO alerts | Model registry/serving | Drift | Load testing
# MAGIC
# MAGIC ### What
# MAGIC The project demonstrates architecture patterns but is intentionally bounded for Free Edition.
# MAGIC
# MAGIC ### Why
# MAGIC Production readiness requires organizational/security/operational controls that cannot be proven in a standalone DBC.
# MAGIC
# MAGIC ### How
# MAGIC Replace synthetic landing generation with enterprise sources, deploy DABs from source control, use environment-specific permissions, and add operational/model SLOs.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC Being explicit about this limitation makes the presentation stronger, not weaker.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Slide 32 — Final Outcome
# MAGIC
# MAGIC ### On-slide text
# MAGIC **One governed retail platform**
# MAGIC
# MAGIC Reliable data + measurable quality + reproducible ML + grounded AI + incremental visibility + deployable automation
# MAGIC
# MAGIC ### What
# MAGIC The final product is a coherent end-to-end retail intelligence architecture.
# MAGIC
# MAGIC ### Why
# MAGIC The value is consistency across data engineering, analytics, ML, AI, realtime, and operations.
# MAGIC
# MAGIC ### How
# MAGIC Every layer is connected through governed Delta/Unity Catalog data products and validated through observable runtime evidence.
# MAGIC
# MAGIC ### Speaker notes
# MAGIC End with the central message: the project is not a collection of unrelated features. It is one governed flow from source data to decisions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Q&A Talking Points
# MAGIC
# MAGIC **Why Bronze/Silver/Gold?** Bronze preserves source history, Silver centralizes trusted business logic, Gold exposes stable consumer products.
# MAGIC
# MAGIC **Why is `Trigger.AvailableNow()` used?** It provides bounded incremental Structured Streaming suitable for the Free Edition serverless notebook/job path and exits cleanly after processing available files.
# MAGIC
# MAGIC **Why also use Lakeflow?** It demonstrates the managed pipeline approach for serverless realtime processing and is deployable through DABs.
# MAGIC
# MAGIC **Why not count cancelled orders in revenue?** A cancellation is an order attempt, not recognized business revenue. Both metrics are retained separately.
# MAGIC
# MAGIC **Why aggregate product sales and reviews separately?** Their different one-to-many relationships would multiply rows if joined at raw grain.
# MAGIC
# MAGIC **Why package StandardScaler inside sklearn Pipelines?** The logged model then contains exactly the preprocessing it needs, reducing training/serving mismatch.
# MAGIC
# MAGIC **Why is the demand output called a baseline?** The method is a deterministic recent-history average, useful as a benchmark but not a calibrated forecast.
# MAGIC
# MAGIC **Is the agent a full LLM?** The core demo is a deterministic, grounded tool-using agent. That keeps the Free Edition project self-contained and makes responses auditable.
# MAGIC
# MAGIC **Can you guarantee it works in every workspace?** No static package can guarantee workspace permissions, quotas, runtime versions, or external service state. That is why the project includes `07_runtime_validation` as the final workspace-side acceptance test.