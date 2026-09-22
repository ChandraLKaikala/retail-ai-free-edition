# Databricks notebook source
# MAGIC %md
# MAGIC # RetailHub AI — Cell-by-Cell Code Explanation
# MAGIC
# MAGIC **Source inspected:** `retail-ai-free-edition-e2e-v4.0.dbc`  
# MAGIC **Coverage:** every notebook and every cell in the DBC, including documentation cells.  
# MAGIC
# MAGIC ## How to read this guide
# MAGIC
# MAGIC For each cell, the guide explains **what** the cell does, **why** it exists, and **how** it contributes to the next stage. Code cells also list the most relevant project tables and implementation mechanisms detected in the cell. The explanations describe intent and behavior; they do not replace runtime validation inside Databricks.
# MAGIC
# MAGIC
# MAGIC # Notebook: `00_START_HERE_FREE_EDITION`
# MAGIC
# MAGIC **Cells:** 4
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Project landing page
# MAGIC
# MAGIC **What it does:** Explains what the package is and establishes the Free Edition boundary: notebooks are inside the DBC, while Jobs/pipelines are resources deployed by the DAB.
# MAGIC
# MAGIC **Why it exists:** Prevents a user from expecting DBC import to create infrastructure automatically.
# MAGIC
# MAGIC **Cell content focus:** START HERE · RetailHub AI — Databricks Free Edition E2E v4.0 This archive is the **Free Edition-safe notebook package**. It uses Python, Unity Catalog managed tables/volumes, serverless-compatible ML/MLflow, metadata-driven quality, a grounded local agent, an incremental Structured Streaming demo using `Trigger.AvailableNow()`, and a Lakeflow pipeline source notebook. Important platform boundary
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `00_START_HERE_FREE_EDITION` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Markdown
# MAGIC
# MAGIC **Purpose:** Manual execution sequence
# MAGIC
# MAGIC **What it does:** Lists the exact notebook order and explains that the AvailableNow streaming notebook is rerunnable for new increments.
# MAGIC
# MAGIC **Why it exists:** Provides a deterministic demo path when the DAB is not used.
# MAGIC
# MAGIC **Cell content focus:** Fastest manual run in Free Edition 1. Import this DBC into your workspace. 2. Run `00_setup`.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `00_START_HERE_FREE_EDITION` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Markdown
# MAGIC
# MAGIC **Purpose:** Free Edition design rationale
# MAGIC
# MAGIC **What it does:** Documents serverless, Unity Catalog Volume, AvailableNow, Lakeflow, and quota-aware choices.
# MAGIC
# MAGIC **Why it exists:** Makes the architecture defensible against platform-limit questions.
# MAGIC
# MAGIC **Cell content focus:** Free Edition design choices - **Serverless only:** no cluster definitions are required. - **Python only:** no R/Scala dependency.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `00_START_HERE_FREE_EDITION` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Markdown
# MAGIC
# MAGIC **Purpose:** Capability inventory
# MAGIC
# MAGIC **What it does:** Summarizes data engineering, quality, ML/MLOps, AI, streaming, monitoring, dashboarding, and deployment features.
# MAGIC
# MAGIC **Why it exists:** Acts as the project scope checklist.
# MAGIC
# MAGIC **Cell content focus:** What is included **Data engineering:** Bronze/Silver/Gold Delta tables, current synthetic data, referentially coherent orders/items/payments/shipments/returns/inventory/reviews. **Quality:** metadata-driven checks, severity/thresholds, latest results, history, runtime validation.
# MAGIC
# MAGIC **How it connects:** This closes `00_START_HERE_FREE_EDITION` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `00_setup`
# MAGIC
# MAGIC **Cells:** 4
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Setup overview
# MAGIC
# MAGIC **What it does:** Describes the schemas/metadata/monitoring resources created and tells the user which realtime notebook is manual versus pipeline-managed.
# MAGIC
# MAGIC **Why it exists:** Prevents the managed pipeline source from being run in the wrong context.
# MAGIC
# MAGIC **Cell content focus:** 00 · Project Setup — Free Edition E2E v4.0 Creates the governed schemas, metadata-driven quality rules, monitoring tables, and a Unity Catalog managed volume used by the Free Edition streaming demo. **Manual notebook order:** `00_setup` → `01_bronze_ingestion` → `02_silver_gold_pipeline` → `03_data_quality_checks` → `04_ml_training` → `05_genai_agent` → `06_realtime_streaming` → `07_runtime_validation`.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `00_setup` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Catalog parameterization and schemas
# MAGIC
# MAGIC **What it does:** Reads the current catalog, exposes a `catalog` widget, validates the identifier, creates nine schemas, and attempts Delta optimize-write/auto-compact configuration.
# MAGIC
# MAGIC **Why it exists:** One parameter lets the same code run in a writable Free Edition catalog without hard-coding `workspace`; identifier validation reduces accidental SQL injection through object names.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `00_setup` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Quality-rule metadata
# MAGIC
# MAGIC **What it does:** Creates `retail_quality.quality_rules` and loads 36 SQL-backed rules spanning uniqueness, completeness, validity, timeliness, referential integrity, and consistency.
# MAGIC
# MAGIC **Why it exists:** Quality logic becomes data-driven: adding/changing a rule does not require rewriting the quality engine.
# MAGIC
# MAGIC **Key code signature:** `from pyspark.sql import Row | spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_quality.quality_rules (rule_id STRING,dataset STRING,field STRING,rule_type STRING,description STRING,severity STRING,threshold DOUBLE,enabled BOOLEAN,check_expression STRING) USING DELTA") | rules = [`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.categories`, `retail_bronze.clickstream_events`, `retail_bronze.customers`, `retail_bronze.inventory_events`, `retail_bronze.order_items`, `retail_bronze.orders`, `retail_bronze.payments`, `retail_bronze.product_reviews`, `retail_bronze.products`, `retail_bronze.returns`, `retail_bronze.shipments`, `retail_bronze.support_tickets`, `retail_quality.quality_rules`
# MAGIC
# MAGIC **Reads:** `retail_bronze.categories`, `retail_bronze.clickstream_events`, `retail_bronze.customers`, `retail_bronze.inventory_events`, `retail_bronze.order_items`, `retail_bronze.orders`, `retail_bronze.payments`, `retail_bronze.product_reviews`, `retail_bronze.products`, `retail_bronze.returns`, `retail_bronze.shipments`, `retail_bronze.support_tickets`
# MAGIC
# MAGIC **Writes/updates:** `retail_quality.quality_rules`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `00_setup` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Monitoring tables and Volume
# MAGIC
# MAGIC **What it does:** Creates `pipeline_runs`, `streaming_metrics`, and the managed Unity Catalog Volume used for landing files/checkpoints; prints readiness information.
# MAGIC
# MAGIC **Why it exists:** Provides durable operational telemetry and a Free Edition-safe governed storage location.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE TABLE IF NOT EXISTS {CAT}.retail_monitoring.pipeline_runs ( | run_id STRING, notebook STRING, layer STRING, status STRING, rows_written LONG,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_monitoring.pipeline_runs`, `retail_monitoring.streaming_metrics`
# MAGIC
# MAGIC **Writes/updates:** `retail_monitoring.pipeline_runs`, `retail_monitoring.streaming_metrics`
# MAGIC
# MAGIC **How it connects:** This closes `00_setup` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `01_bronze_ingestion`
# MAGIC
# MAGIC **Cells:** 18
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Bronze contract
# MAGIC
# MAGIC **What it does:** States the deterministic row count, table count, current-date anchor, and realism guarantees.
# MAGIC
# MAGIC **Why it exists:** Sets expectations before data generation.
# MAGIC
# MAGIC **Cell content focus:** 01 · Bronze Ingestion — Free Edition E2E v4.0 Creates **326,308 deterministic synthetic rows across 15 Bronze tables**, anchored to the Spark session's current date. The generator enforces stronger realism: age-aware order states, coherent payment state, cancelled orders are never shipped, every `Returned` order has an approved purchased-product return, inventory begins with opening stock and cannot become physically negative, refunds are capped to purchased net value, and verified reviews are t
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Run context and reusable writer
# MAGIC
# MAGIC **What it does:** Creates catalog widget validation, run/batch IDs, current UTC timestamp, fixed random seed, current-date anchor, deterministic `rng()` helper, and a `save()` helper that writes Delta tables with audit columns.
# MAGIC
# MAGIC **Why it exists:** Centralizes reproducibility and audit metadata so every Bronze table follows the same ingestion contract.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Category master
# MAGIC
# MAGIC **What it does:** Creates 8 product categories and writes `retail_bronze.categories`.
# MAGIC
# MAGIC **Why it exists:** Provides a stable product taxonomy referenced by products and quality rules.
# MAGIC
# MAGIC **Key code signature:** `save(spark.createDataFrame([ | ("CAT001","Electronics","Phones and computers"),("CAT002","Clothing","Apparel"), | ("CAT003","Home & Kitchen","Household"),("CAT004","Sports","Sports and outdoors"),`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Supplier master
# MAGIC
# MAGIC **What it does:** Generates 150 deterministic suppliers with country, rating, lead time, and status, then writes `suppliers`.
# MAGIC
# MAGIC **Why it exists:** Creates upstream supply-chain attributes for product and supplier-performance analysis.
# MAGIC
# MAGIC **Key code signature:** `SNAMES = ["Apex","Nexus","Prime","Global","Delta","Pacific","Atlantic","Summit","Pinnacle","Elite"] | SCOUNTRIES = ["US","CN","DE","IN","JP","GB","FR","CA","AU","BR"] | sup = [Row(supplier_id=f"SUP{i:04d}", name=f"{rng(i,100).choice(SNAMES)} Corp {i}",`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Store master
# MAGIC
# MAGIC **What it does:** Generates 50 US stores with location, type, size, open date, and active status.
# MAGIC
# MAGIC **Why it exists:** Provides a physical retail dimension for store/channel performance.
# MAGIC
# MAGIC **Key code signature:** `STORE_LOCS = [ | ("New York","NY"),("Los Angeles","CA"),("Chicago","IL"),("Houston","TX"), | ("Phoenix","AZ"),("Philadelphia","PA"),("San Antonio","TX"),("San Diego","CA")`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Code
# MAGIC
# MAGIC **Purpose:** Product catalog
# MAGIC
# MAGIC **What it does:** Generates 1,000 products with category/supplier foreign keys, price, cost, SKU, margin inputs, weight, and status.
# MAGIC
# MAGIC **Why it exists:** Supplies the economic and dimensional attributes used by order lines, inventory, reviews, and Gold product KPIs.
# MAGIC
# MAGIC **Key code signature:** `PNAMES = ["Pro","Plus","Max","Ultra","Mini","Lite","Elite","Smart","Classic","Premium"] | PTYPES = ["Widget","Device","Kit","Pack","Set","Bundle","System","Unit","Module","Hub"] | prods = []`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 07 — Code
# MAGIC
# MAGIC **Purpose:** Customer master
# MAGIC
# MAGIC **What it does:** Generates 10,000 customers with coherent country/city pairs, segments, registration dates, status, and loyalty points.
# MAGIC
# MAGIC **Why it exists:** Creates the customer population for Customer 360, churn, segmentation, support, web activity, and agent lookups.
# MAGIC
# MAGIC **Key code signature:** `FNAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah","Thomas","Karen","Charles","Lisa"] | LNAMES = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Moore"] | SEGS   = ["Premium","Standard","Basic","VIP","Standard","Standard"]`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 08 — Code
# MAGIC
# MAGIC **Purpose:** Order headers
# MAGIC
# MAGIC **What it does:** Generates 30,000 orders with age-aware lifecycle states so very recent orders can still be processing/shipped while older orders may be completed/returned/cancelled.
# MAGIC
# MAGIC **Why it exists:** Status realism prevents obviously impossible lifecycle states and supports time-aware demos.
# MAGIC
# MAGIC **Key code signature:** `CHANS   = ["web","mobile","store","mobile","web"] | CIDS    = [f"C{i:07d}" for i in range(1,10001)] | SIDS    = [f"STR{i:04d}" for i in range(1,51)]`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 09 — Code
# MAGIC
# MAGIC **Purpose:** Order items and header reconciliation
# MAGIC
# MAGIC **What it does:** Generates 75,000 line items using actual product prices, guarantees every order gets a line, writes items, then MERGEs line totals/discounts/tax back into order headers.
# MAGIC
# MAGIC **Why it exists:** Makes financial fields internally reconcilable instead of independently random.
# MAGIC
# MAGIC **Key code signature:** `from collections import defaultdict | PIDS = [f"PRD{i:05d}" for i in range(1,1001)] | OIDS = [f"ORD{i:08d}" for i in range(1,30001)]`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.order_items`, `retail_bronze.orders`
# MAGIC
# MAGIC **Reads:** `retail_bronze.order_items`, `retail_bronze.orders`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.orders`
# MAGIC
# MAGIC **Important mechanisms:** Delta MERGE
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 10 — Code
# MAGIC
# MAGIC **Purpose:** Payments
# MAGIC
# MAGIC **What it does:** Creates one primary payment per order plus failed attempts. Non-cancelled orders have completed payments; cancelled orders are represented as refunded. Amounts use the reconciled payable amount.
# MAGIC
# MAGIC **Why it exists:** Keeps payment state consistent with order state and prevents fake recognized revenue.
# MAGIC
# MAGIC **Key code signature:** `PMETHODS = ["credit_card","debit_card","paypal","apple_pay","bank_transfer"] | pays = [] | for i in range(1,32001):`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 11 — Code
# MAGIC
# MAGIC **Purpose:** Shipments
# MAGIC
# MAGIC **What it does:** Creates shipments only for non-cancelled orders and derives processing/in-transit/delivered states from order age/status with bounded dates.
# MAGIC
# MAGIC **Why it exists:** Ensures cancelled orders are never shipped and completed/returned orders can have plausible delivery history.
# MAGIC
# MAGIC **Key code signature:** `CARRIERS = ["FedEx","UPS","USPS","DHL","Amazon Logistics"] | SHIP_ELIGIBLE_OIDS = [oid for oid in OIDS if ORDER_STATUS[oid] != "Cancelled"] | ships = []`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 12 — Code
# MAGIC
# MAGIC **Purpose:** Inventory event ledger
# MAGIC
# MAGIC **What it does:** Seeds opening stock for every product/warehouse pair, then generates receipts, sales, returns, transfers/adjustments while constraining negative movements to available stock.
# MAGIC
# MAGIC **Why it exists:** Preserves signed inventory semantics without allowing impossible negative physical stock.
# MAGIC
# MAGIC **Key code signature:** `WAREHOUSES = ["WH001","WH002","WH003","WH004","WH005"] | ITYPES     = ["receipt","sale","adjustment","return","transfer"] | invs = []`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 13 — Code
# MAGIC
# MAGIC **Purpose:** Returns
# MAGIC
# MAGIC **What it does:** Builds eligible order/product pairs, guarantees every `Returned` order has an approved return, fills remaining return rows from valid purchases, and caps refund amounts at purchased net value.
# MAGIC
# MAGIC **Why it exists:** Makes returns/refunds relationally and financially consistent.
# MAGIC
# MAGIC **Key code signature:** `REASONS = ["Defective","Wrong item","Changed mind","Size issue","Quality issue","Damaged shipping"] | from collections import defaultdict | eligible_by_order = defaultdict(list)`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 14 — Code
# MAGIC
# MAGIC **Purpose:** Clickstream
# MAGIC
# MAGIC **What it does:** Generates 100,000 recent clickstream events in four write batches, including session, page, event type, browser, optional product, and duration.
# MAGIC
# MAGIC **Why it exists:** Provides behavioral data while avoiding a single huge local Python list/write operation.
# MAGIC
# MAGIC **Key code signature:** `PAGES    = ["home","product","cart","checkout","search","account","wishlist","category"] | BROWSERS = ["Chrome","Safari","Firefox","Edge","Mobile Safari"] | ETYPES   = ["view","click","add_to_cart","purchase","search"]`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.clickstream_events`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.clickstream_events`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 15 — Code
# MAGIC
# MAGIC **Purpose:** Support tickets
# MAGIC
# MAGIC **What it does:** Generates 3,000 customer-linked tickets with category, priority, state, dates, and optional satisfaction scores.
# MAGIC
# MAGIC **Why it exists:** Feeds support-operation KPIs and provides another operational domain.
# MAGIC
# MAGIC **Key code signature:** `TCATS  = ["billing","shipping","product","account","returns","technical"] | TPRIS  = ["low","medium","high","critical"] | TSTAT  = ["resolved","resolved","resolved","open","pending","escalated"]`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 16 — Code
# MAGIC
# MAGIC **Purpose:** Product reviews
# MAGIC
# MAGIC **What it does:** Generates verified and unverified reviews. Verified reviews choose a product actually purchased by that customer on an eligible order and set a valid review date.
# MAGIC
# MAGIC **Why it exists:** Prevents the common synthetic-data contradiction of 'verified purchase' without a purchase.
# MAGIC
# MAGIC **Key code signature:** `REVIEWABLE_OIDS = [oid for oid in OIDS if ORDER_DATES[oid] < BASE and ORDER_STATUS[oid] in ("Completed","Returned")] | revs = [] | for i in range(1,2001):`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 17 — Code
# MAGIC
# MAGIC **Purpose:** Knowledge documents
# MAGIC
# MAGIC **What it does:** Creates 100 active policies/manuals/runbooks/guides/FAQs across business topics.
# MAGIC
# MAGIC **Why it exists:** Supplies governed text for the retrieval/knowledge agent instead of relying on an external LLM or web source.
# MAGIC
# MAGIC **Key code signature:** `DTYPES = ["policy","manual","runbook","guide","faq"] | TOPICS = ["returns","shipping","billing","loyalty","privacy","security","product_care","account","promotions","warranty","international","b2b","sustainability","accessibility","api"] | docs = []`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `01_bronze_ingestion` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 18 — Code
# MAGIC
# MAGIC **Purpose:** Bronze run telemetry
# MAGIC
# MAGIC **What it does:** Writes a successful Bronze run record to `pipeline_runs` with the expected 326,308 baseline rows.
# MAGIC
# MAGIC **Why it exists:** Lets operations/dashboard/agent queries see ingestion history.
# MAGIC
# MAGIC **Key code signature:** `TOTAL_ROWS = 326_308 | spark.sql(f""" | INSERT INTO {CAT}.retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **Project objects referenced:** `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **Writes/updates:** `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **How it connects:** This closes `01_bronze_ingestion` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `02_silver_gold_pipeline`
# MAGIC
# MAGIC **Cells:** 20
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Curated-layer contract
# MAGIC
# MAGIC **What it does:** Explains that Silver standardizes data while Gold creates business products, with refund-aware revenue and corrected product joins.
# MAGIC
# MAGIC **Why it exists:** Documents the business semantics before transformation code runs.
# MAGIC
# MAGIC **Cell content focus:** 02 - Silver & Gold · Curated Analytics Layer Transforms Bronze into typed Silver facts/dimensions and business-facing Gold products. Revenue is refund-aware, cancelled orders recognize zero revenue, product KPIs aggregate sales/reviews/refunds independently to avoid many-to-many inflation, and dates are explicitly typed. The batch Gold layer is complemented by `06_realtime_streaming`, which maintains separate live minute-level KPIs without rebuilding historical tables.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Run context
# MAGIC
# MAGIC **What it does:** Validates the catalog, creates run metadata, and establishes a timestamp for pipeline logging.
# MAGIC
# MAGIC **Why it exists:** Keeps all transformation objects catalog-parameterized and auditable.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Date dimension
# MAGIC
# MAGIC **What it does:** Creates a multi-year `dim_date` with calendar keys, names, weekend flag, and season.
# MAGIC
# MAGIC **Why it exists:** Centralizes calendar attributes for time-series analysis.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_date AS | SELECT cast(date_format(d,'yyyyMMdd') as int) as date_key, d as full_date,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_silver.dim_date`
# MAGIC
# MAGIC **Writes/updates:** `retail_silver.dim_date`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Customer dimension
# MAGIC
# MAGIC **What it does:** Normalizes email, converts dates, builds full name, preserves segment/status/loyalty, and adds current-record/SCD-style fields.
# MAGIC
# MAGIC **Why it exists:** Provides a clean customer dimension for downstream facts, analytics, and ML.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_customers AS | SELECT customer_id, first_name, last_name, concat(first_name,' ',last_name) as full_name,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.customers`, `retail_silver.dim_customers`
# MAGIC
# MAGIC **Reads:** `retail_bronze.customers`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.customers`, `retail_silver.dim_customers`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Product dimension
# MAGIC
# MAGIC **What it does:** Joins products to categories and derives gross margin and margin percentage.
# MAGIC
# MAGIC **Why it exists:** Creates the business-ready product dimension used by product, demand, and inventory outputs.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_products AS SELECT p.product_id, p.name as product_name, p.category_id, c.name as category_name, p.supplier_id, p.price, p.cost, p.price-p.cost as gross_margin, round((p.price-p.cost)/p.price*100,2) as margin_pct, p.sku, p.weight_kg, p.status FROM {CAT}.retail_bronze.products p LEFT JOIN {CAT}.retail_bronze.categories c ON p.category_id=c.category_id") | print(f"dim_products: {spark.table(f'{CAT}.retail_silver.dim_products').count()}")`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.categories`, `retail_bronze.products`, `retail_silver.dim_products`
# MAGIC
# MAGIC **Reads:** `retail_bronze.categories`, `retail_bronze.products`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.products`, `retail_silver.dim_products`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Code
# MAGIC
# MAGIC **Purpose:** Store and supplier dimensions
# MAGIC
# MAGIC **What it does:** Types store dates and enriches suppliers with a rating-based tier.
# MAGIC
# MAGIC **Why it exists:** Creates reusable dimensions for operational performance analysis.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_stores AS | SELECT store_id, name as store_name, city, state, country, store_type, size_sqft,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.stores`, `retail_bronze.suppliers`, `retail_silver.dim_stores`, `retail_silver.dim_suppliers`
# MAGIC
# MAGIC **Reads:** `retail_bronze.stores`, `retail_bronze.suppliers`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.stores`, `retail_bronze.suppliers`, `retail_silver.dim_stores`, `retail_silver.dim_suppliers`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 07 — Code
# MAGIC
# MAGIC **Purpose:** Order fact with recognized revenue
# MAGIC
# MAGIC **What it does:** Aggregates approved refunds by order, types dates/amounts, computes net sales before refunds, approved refund amount, and `net_revenue`; cancelled orders recognize zero.
# MAGIC
# MAGIC **Why it exists:** Defines revenue once and correctly so Gold, ML, dashboard, and AI use the same semantics.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_orders AS | WITH approved_refunds AS (`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.orders`, `retail_bronze.returns`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Reads:** `retail_bronze.orders`, `retail_bronze.returns`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.orders`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 08 — Code
# MAGIC
# MAGIC **Purpose:** Order-item fact
# MAGIC
# MAGIC **What it does:** Filters invalid lines and derives net line value after line discount.
# MAGIC
# MAGIC **Why it exists:** Provides the product-grain sales basis used by product KPIs and demand baseline.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_order_items AS SELECT item_id, order_id, product_id, quantity, unit_price, line_total, discount_pct, round(line_total*(1-discount_pct),2) as net_line_total FROM {CAT}.retail_bronze.order_items WHERE quantity>0 AND unit_price>0") | print(f"fact_order_items: {spark.table(f'{CAT}.retail_silver.fact_order_items').count()}")`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.order_items`, `retail_silver.fact_order_items`
# MAGIC
# MAGIC **Reads:** `retail_bronze.order_items`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.order_items`, `retail_silver.fact_order_items`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 09 — Code
# MAGIC
# MAGIC **Purpose:** Payment and shipment facts
# MAGIC
# MAGIC **What it does:** Standardizes payment fields and shipment dates; derives `delivered_on_time` when actual delivery exists.
# MAGIC
# MAGIC **Why it exists:** Creates clean operational facts for reconciliation and delivery analysis.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_payments AS | SELECT payment_id, order_id, amount, method, status as payment_status,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.payments`, `retail_bronze.shipments`, `retail_silver.fact_payments`, `retail_silver.fact_shipments`
# MAGIC
# MAGIC **Reads:** `retail_bronze.payments`, `retail_bronze.shipments`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.payments`, `retail_bronze.shipments`, `retail_silver.fact_payments`, `retail_silver.fact_shipments`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 10 — Code
# MAGIC
# MAGIC **Purpose:** Return and inventory facts
# MAGIC
# MAGIC **What it does:** Types return dates, derives days-to-return, and aggregates signed inventory events into daily product/warehouse snapshots with received/sold totals.
# MAGIC
# MAGIC **Why it exists:** Converts event-level operational data into analytics-friendly facts.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_returns AS | SELECT r.return_id, r.order_id, r.product_id, r.reason, to_date(r.return_date) as return_date,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.inventory_events`, `retail_bronze.orders`, `retail_bronze.returns`, `retail_silver.fact_inventory_snapshot`, `retail_silver.fact_returns`
# MAGIC
# MAGIC **Reads:** `retail_bronze.inventory_events`, `retail_bronze.orders`, `retail_bronze.returns`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.returns`, `retail_silver.fact_inventory_snapshot`, `retail_silver.fact_returns`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 11 — Code
# MAGIC
# MAGIC **Purpose:** Daily sales Gold mart
# MAGIC
# MAGIC **What it does:** Groups by order date and channel, separately retaining attempts, recognized orders, customers, revenue, discounts, cancellations, and AOV.
# MAGIC
# MAGIC **Why it exists:** Supports channel/time dashboards while keeping cancellations analytically visible.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_gold.daily_sales_summary AS | SELECT order_date, channel,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.daily_sales_summary`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Reads:** `retail_silver.fact_orders`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.daily_sales_summary`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 12 — Code
# MAGIC
# MAGIC **Purpose:** Customer KPI mart
# MAGIC
# MAGIC **What it does:** Aggregates recognized orders/revenue/AOV, latest order date, recency, tenure, and loyalty attributes per customer.
# MAGIC
# MAGIC **Why it exists:** Creates the feature/KPI foundation for Customer 360 and ML.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_gold.customer_kpis AS | SELECT c.customer_id, c.full_name, c.customer_segment, c.country, c.status, c.registration_date, c.loyalty_points,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_kpis`, `retail_silver.dim_customers`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Reads:** `retail_silver.dim_customers`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.customer_kpis`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 13 — Code
# MAGIC
# MAGIC **Purpose:** Customer lifetime value baseline
# MAGIC
# MAGIC **What it does:** Annualizes historical revenue over tenure with a 30-day floor, extrapolates three years, and assigns CLV tiers.
# MAGIC
# MAGIC **Why it exists:** Provides an interpretable demo CLV baseline while labeling its method explicitly.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_gold.customer_lifetime_value AS | SELECT customer_id, full_name, customer_segment, total_revenue as historical_revenue, total_orders, avg_order_value, tenure_days,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_kpis`, `retail_gold.customer_lifetime_value`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_kpis`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.customer_lifetime_value`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 14 — Code
# MAGIC
# MAGIC **Purpose:** Customer 360
# MAGIC
# MAGIC **What it does:** Combines customer KPIs and CLV, then derives simple recency-based churn-risk labels.
# MAGIC
# MAGIC **Why it exists:** Creates a single business-facing customer record for dashboard, agent, and ML feature extraction.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.customer_360 AS SELECT ck.customer_id, ck.full_name, ck.customer_segment, ck.country, ck.status, ck.total_orders, ck.total_revenue, ck.avg_order_value, ck.days_since_last_order, ck.tenure_days, ck.loyalty_points, clv.predicted_3yr_clv, clv.clv_tier, CASE WHEN ck.days_since_last_order>365 THEN 'High' WHEN ck.days_since_last_order>180 THEN 'Medium' ELSE 'Low' END as churn_risk FROM {CAT}.retail_gold.customer_kpis ck LEFT JOIN {CAT}.retail_gold.customer_lifetime_value clv ON ck.customer_id=clv.customer_id") | print(f"customer_360: {spark.table`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_360`, `retail_gold.customer_kpis`, `retail_gold.customer_lifetime_value`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_kpis`, `retail_gold.customer_lifetime_value`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.customer_360`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 15 — Code
# MAGIC
# MAGIC **Purpose:** Product KPI mart
# MAGIC
# MAGIC **What it does:** Aggregates sales, refunds, and reviews independently before joining to product dimension; subtracts approved refunds and floors revenue at zero.
# MAGIC
# MAGIC **Why it exists:** Avoids many-to-many join multiplication that would otherwise inflate units/revenue.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_gold.product_kpis AS | WITH sales AS (`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.product_reviews`, `retail_gold.product_kpis`, `retail_silver.dim_products`, `retail_silver.fact_order_items`, `retail_silver.fact_orders`, `retail_silver.fact_returns`
# MAGIC
# MAGIC **Reads:** `retail_bronze.product_reviews`, `retail_silver.dim_products`, `retail_silver.fact_order_items`, `retail_silver.fact_orders`, `retail_silver.fact_returns`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.product_kpis`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 16 — Code
# MAGIC
# MAGIC **Purpose:** Store and supplier performance
# MAGIC
# MAGIC **What it does:** Builds store attempts/recognized orders/customers/revenue/AOV and supplier product-count metrics.
# MAGIC
# MAGIC **Why it exists:** Adds location and supply-chain business views.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_gold.store_performance AS | SELECT s.store_id, s.store_name, s.city, s.state, s.store_type,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.products`, `retail_gold.store_performance`, `retail_gold.supplier_performance`, `retail_silver.dim_stores`, `retail_silver.dim_suppliers`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Reads:** `retail_bronze.products`, `retail_silver.dim_stores`, `retail_silver.dim_suppliers`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.store_performance`, `retail_gold.supplier_performance`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 17 — Code
# MAGIC
# MAGIC **Purpose:** Inventory health
# MAGIC
# MAGIC **What it does:** Rolls snapshots by product/warehouse and classifies stock as Out of Stock/Critical/Low/Normal/Overstocked with a reorder flag.
# MAGIC
# MAGIC **Why it exists:** Converts raw stock movement into an actionable inventory status.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.inventory_health AS SELECT i.product_id, p.product_name, p.category_name, i.warehouse_id, SUM(i.net_quantity) as current_stock, SUM(i.total_sold) as total_sold, CASE WHEN SUM(i.net_quantity)<=0 THEN 'Out of Stock' WHEN SUM(i.net_quantity)<=10 THEN 'Critical' WHEN SUM(i.net_quantity)<=50 THEN 'Low' WHEN SUM(i.net_quantity)<=200 THEN 'Normal' ELSE 'Overstocked' END as stock_status, CASE WHEN SUM(i.net_quantity)<=10 THEN true ELSE false END as reorder_flag FROM {CAT}.retail_silver.fact_inventory_snapshot i LEFT JOIN {CAT}.retail_silver.dim_pro`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.inventory_health`, `retail_silver.dim_products`, `retail_silver.fact_inventory_snapshot`
# MAGIC
# MAGIC **Reads:** `retail_silver.dim_products`, `retail_silver.fact_inventory_snapshot`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.inventory_health`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 18 — Code
# MAGIC
# MAGIC **Purpose:** Support operations
# MAGIC
# MAGIC **What it does:** Aggregates tickets by category/priority/status with satisfaction and resolution statistics.
# MAGIC
# MAGIC **Why it exists:** Makes customer-service performance visible as a Gold data product.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.support_operations AS SELECT category, priority, status as ticket_status, COUNT(*) as ticket_count, round(AVG(satisfaction_score),2) as avg_satisfaction, COUNT(CASE WHEN resolved_date IS NOT NULL THEN 1 END) as resolved_count, round(COUNT(CASE WHEN resolved_date IS NOT NULL THEN 1 END)*100.0/COUNT(*),2) as resolution_rate_pct FROM {CAT}.retail_bronze.support_tickets GROUP BY category,priority,status") | print(f"support_operations: {spark.table(f'{CAT}.retail_gold.support_operations').count()}")`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.support_tickets`, `retail_gold.support_operations`
# MAGIC
# MAGIC **Reads:** `retail_bronze.support_tickets`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.support_operations`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 19 — Code
# MAGIC
# MAGIC **Purpose:** Executive KPI
# MAGIC
# MAGIC **What it does:** Creates a one-row executive snapshot covering customers, recognized orders, attempts, revenue, AOV, returns, products, and open tickets.
# MAGIC
# MAGIC **Why it exists:** Provides a stable source for executive dashboard and agent summaries.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_gold.executive_kpi AS | SELECT current_date() as report_date,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.support_tickets`, `retail_gold.executive_kpi`, `retail_silver.dim_customers`, `retail_silver.dim_products`, `retail_silver.fact_orders`, `retail_silver.fact_returns`
# MAGIC
# MAGIC **Reads:** `retail_bronze.support_tickets`, `retail_silver.dim_customers`, `retail_silver.dim_products`, `retail_silver.fact_orders`, `retail_silver.fact_returns`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.executive_kpi`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `02_silver_gold_pipeline` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 20 — Code
# MAGIC
# MAGIC **Purpose:** Pipeline telemetry
# MAGIC
# MAGIC **What it does:** Counts key curated outputs and appends a successful Silver/Gold run to `pipeline_runs`.
# MAGIC
# MAGIC **Why it exists:** Makes the batch transformation observable to operations and the agent.
# MAGIC
# MAGIC **Key code signature:** `curated_rows = sum([ | spark.table(f"{CAT}.retail_silver.fact_orders").count(), | spark.table(f"{CAT}.retail_silver.fact_order_items").count(),`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_360`, `retail_gold.product_kpis`, `retail_monitoring.pipeline_runs`, `retail_silver.fact_order_items`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Writes/updates:** `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **How it connects:** This closes `02_silver_gold_pipeline` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `03_data_quality_checks`
# MAGIC
# MAGIC **Cells:** 3
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Quality framework overview
# MAGIC
# MAGIC **What it does:** Documents the metadata-driven design, 36 active rules, rule types, severity levels, and outputs.
# MAGIC
# MAGIC **Why it exists:** Explains why checks live in metadata rather than hardcoded procedural branches.
# MAGIC
# MAGIC **Cell content focus:** 03 - Data Quality Checks Metadata-Driven Quality Framework **How it works:** This notebook reads all enabled rules from `retail_quality.quality_rules` and dynamically executes each rule's `check_expression` SQL against the Bronze layer. No hardcoded checks — add or modify rules in the table and re-run.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `03_data_quality_checks` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Dynamic quality engine
# MAGIC
# MAGIC **What it does:** Creates latest/history result tables, loads all enabled rules, executes each rule's SQL, calculates failure percentage versus threshold, distinguishes PASS/FAIL/ERROR, overwrites latest results, and appends history.
# MAGIC
# MAGIC **Why it exists:** A single engine can execute all current/future rules and preserves both current state and audit history.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Project objects referenced:** `retail_quality.quality_results`, `retail_quality.quality_results_history`, `retail_quality.quality_rules`
# MAGIC
# MAGIC **Reads:** `retail_quality.quality_rules`
# MAGIC
# MAGIC **Writes/updates:** `retail_quality.quality_results`, `retail_quality.quality_results_history`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `03_data_quality_checks` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Quality rollups and telemetry
# MAGIC
# MAGIC **What it does:** Builds severity-level `quality_summary`, the Gold `data_quality_kpi`, logs engine status to `pipeline_runs`, and reports counts.
# MAGIC
# MAGIC **Why it exists:** Turns technical validation into executive-quality metrics and operational monitoring.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_quality.quality_summary AS | SELECT severity, COUNT(*) AS rule_count,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.data_quality_kpi`, `retail_monitoring.pipeline_runs`, `retail_quality.quality_results`, `retail_quality.quality_summary`
# MAGIC
# MAGIC **Reads:** `retail_quality.quality_results`, `retail_quality.quality_summary`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.data_quality_kpi`, `retail_monitoring.pipeline_runs`, `retail_quality.quality_summary`
# MAGIC
# MAGIC **How it connects:** This closes `03_data_quality_checks` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `04_ml_training`
# MAGIC
# MAGIC **Cells:** 10
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** ML scope
# MAGIC
# MAGIC **What it does:** States that preprocessing is packaged in sklearn Pipelines, MLflow is per-user, and demand is a baseline rather than a calibrated forecast.
# MAGIC
# MAGIC **Why it exists:** Sets accurate expectations about the demonstration.
# MAGIC
# MAGIC **Cell content focus:** 04 · Machine Learning + MLflow — Free Edition E2E v4.0 Trains demonstration churn, segmentation, and anomaly models with preprocessing packaged inside each sklearn Pipeline. The notebook uses a per-user MLflow experiment by default, avoiding a dependency on shared workspace folders. The demand output is a deterministic 90-day daily-average baseline; it is not presented as a calibrated forecasting model.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** ML context and compatibility helpers
# MAGIC
# MAGIC **What it does:** Configures catalog/MLflow experiment, imports sklearn/MLflow, creates run metadata, defines model logging compatible with newer/older MLflow APIs, and safely extracts positive-class probability.
# MAGIC
# MAGIC **Why it exists:** Makes model runs reproducible and reduces runtime-version friction.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** MLflow, Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Feature engineering and train/test split
# MAGIC
# MAGIC **What it does:** Reads Customer 360 + dimension status, creates numeric features and a synthetic churn label, converts to pandas, and creates an 80/20 stratified split when possible.
# MAGIC
# MAGIC **Why it exists:** Provides a consistent supervised learning dataset from governed Gold/Silver data.
# MAGIC
# MAGIC **Key code signature:** `_sql = f""" | SELECT c.customer_id, | COALESCE(c.tenure_days,0) as tenure_days,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_360`, `retail_silver.dim_customers`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_360`, `retail_silver.dim_customers`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Churn Random Forest
# MAGIC
# MAGIC **What it does:** Fits StandardScaler + balanced RandomForest, evaluates accuracy/AUC, logs parameters/metrics/model to MLflow, and keeps the fitted Pipeline for full-population scoring.
# MAGIC
# MAGIC **Why it exists:** Packaging preprocessing with the model avoids train/serve skew.
# MAGIC
# MAGIC **Key code signature:** `with mlflow.start_run(run_name="churn_rf_v3_2"): | churn_pipeline = Pipeline([ | ("scale", StandardScaler()),`
# MAGIC
# MAGIC **Important mechanisms:** MLflow, sklearn Pipeline
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** K-Means segmentation
# MAGIC
# MAGIC **What it does:** Fits StandardScaler + KMeans(k=4), logs inertia/silhouette/model, profiles clusters, and maps cluster IDs to business labels based on observed value score.
# MAGIC
# MAGIC **Why it exists:** Cluster IDs are arbitrary; profiling them prevents misleading fixed-label assumptions.
# MAGIC
# MAGIC **Key code signature:** `SEG_FEAT = ["total_revenue","total_orders","days_since_last_order","loyalty_points"] | segmentation_pipeline = Pipeline([ | ("scale", StandardScaler()),`
# MAGIC
# MAGIC **Important mechanisms:** MLflow, sklearn Pipeline
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Code
# MAGIC
# MAGIC **Purpose:** Isolation Forest
# MAGIC
# MAGIC **What it does:** Fits StandardScaler + IsolationForest, flags roughly 5% contamination, reverses decision-function sign for intuitive anomaly direction, logs metrics/model.
# MAGIC
# MAGIC **Why it exists:** Adds unsupervised risk detection with reproducible preprocessing.
# MAGIC
# MAGIC **Key code signature:** `anomaly_pipeline = Pipeline([ | ("scale", StandardScaler()), | ("model", IsolationForest(contamination=0.05, random_state=42, n_estimators=150, n_jobs=-1))`
# MAGIC
# MAGIC **Important mechanisms:** MLflow, sklearn Pipeline
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 07 — Code
# MAGIC
# MAGIC **Purpose:** Churn prediction output
# MAGIC
# MAGIC **What it does:** Scores all customers, stores probability/prediction/actual label/cluster/anomaly fields with model version and timestamp in `retail_ml.churn_predictions`.
# MAGIC
# MAGIC **Why it exists:** Creates a governed serving/output table rather than leaving results only in notebook memory.
# MAGIC
# MAGIC **Key code signature:** `pdf["churn_probability"] = positive_class_probability(churn_pipeline, pdf[FEAT]) | pdf["churn_predicted"] = (pdf["churn_probability"]>=0.5).astype(int) | (spark.createDataFrame(pdf[["customer_id","churn_probability","churn_predicted","churn_label","cluster","anomaly_score_raw","is_anomaly"]]`
# MAGIC
# MAGIC **Project objects referenced:** `retail_ml.churn_predictions`
# MAGIC
# MAGIC **Writes/updates:** `retail_ml.churn_predictions`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 08 — Code
# MAGIC
# MAGIC **Purpose:** Customer segment output
# MAGIC
# MAGIC **What it does:** Maps numeric cluster IDs through the derived label dictionary and writes `retail_ml.customer_segments`.
# MAGIC
# MAGIC **Why it exists:** Makes segmentation readable to business users and dashboards.
# MAGIC
# MAGIC **Key code signature:** `seg_udf = udf(lambda x: SEG.get(int(x),"Unknown"), StringType()) | (spark.createDataFrame(pdf[["customer_id","cluster"]].rename(columns={"cluster":"segment_id"})) | .withColumn("segment_label", seg_udf("segment_id"))`
# MAGIC
# MAGIC **Project objects referenced:** `retail_ml.customer_segments`
# MAGIC
# MAGIC **Writes/updates:** `retail_ml.customer_segments`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 09 — Code
# MAGIC
# MAGIC **Purpose:** Anomaly output + demand baseline
# MAGIC
# MAGIC **What it does:** Writes anomaly scores and creates a 90-calendar-day product demand baseline for the next 30 days with a stability score.
# MAGIC
# MAGIC **Why it exists:** Provides both operational anomaly data and an interpretable demand planning baseline.
# MAGIC
# MAGIC **Key code signature:** `(spark.createDataFrame(pdf[["customer_id","anomaly_score_raw","is_anomaly"]]) | .write.format("delta").mode("overwrite").option("overwriteSchema","true") | .saveAsTable(f"{CAT}.retail_ml.anomaly_scores"))`
# MAGIC
# MAGIC **Project objects referenced:** `retail_ml.anomaly_scores`, `retail_ml.demand_forecast`, `retail_silver.dim_products`, `retail_silver.fact_order_items`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Reads:** `retail_silver.dim_products`, `retail_silver.fact_order_items`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Writes/updates:** `retail_ml.anomaly_scores`, `retail_ml.demand_forecast`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `04_ml_training` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 10 — Code
# MAGIC
# MAGIC **Purpose:** ML summaries/monitoring
# MAGIC
# MAGIC **What it does:** Creates Gold/monitoring summary outputs and logs the ML notebook run.
# MAGIC
# MAGIC **Why it exists:** Makes model results visible outside the training notebook and connects ML to project observability.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.anomaly_summary AS SELECT is_anomaly, COUNT(*) as count, round(AVG(churn_probability),4) as avg_churn FROM {CAT}.retail_ml.churn_predictions GROUP BY is_anomaly") | spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.anomaly_kpi AS SELECT current_date() as report_date, COUNT(*) as total_scored, SUM(is_anomaly) as anomaly_count, round(SUM(is_anomaly)*100.0/COUNT(*),2) as anomaly_rate_pct, round(AVG(churn_probability),4) as avg_churn_risk FROM {CAT}.retail_ml.churn_predictions") | spark.sql(f"""`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.anomaly_kpi`, `retail_gold.anomaly_summary`, `retail_ml.churn_predictions`, `retail_monitoring.anomaly_events`, `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **Reads:** `retail_ml.churn_predictions`
# MAGIC
# MAGIC **Writes/updates:** `retail_gold.anomaly_kpi`, `retail_gold.anomaly_summary`, `retail_ml.churn_predictions`, `retail_monitoring.anomaly_events`, `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **How it connects:** This closes `04_ml_training` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `05_genai_agent`
# MAGIC
# MAGIC **Cells:** 17
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Agent scope
# MAGIC
# MAGIC **What it does:** States that the agent is grounded, deterministic, schema-aligned, read-only, and evaluated.
# MAGIC
# MAGIC **Why it exists:** Avoids overstating the system as an unconstrained generative model.
# MAGIC
# MAGIC **Cell content focus:** 05 - GenAI & Agentic System · Grounded and Schema-Aligned A deterministic tool-using assistant over the lakehouse. The optimized version aligns every tool with the tables actually produced by the pipeline, enforces read-only SQL, avoids raw user-query SQL interpolation for retrieval, writes a consistent interaction log, and creates a measured routing/source-support evaluation table.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Markdown
# MAGIC
# MAGIC **Purpose:** Architecture diagram
# MAGIC
# MAGIC **What it does:** Shows safety filter -> orchestrator -> specialist agents -> governed tools.
# MAGIC
# MAGIC **Why it exists:** Gives the reader a mental model before implementation details.
# MAGIC
# MAGIC **Cell content focus:** 05 · GenAI Knowledge Base & Agentic AI System Architecture ```
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Agent run context
# MAGIC
# MAGIC **What it does:** Validates catalog, creates run metadata, imports Spark/time utilities.
# MAGIC
# MAGIC **Why it exists:** Keeps tool table references portable across writable catalogs.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Markdown
# MAGIC
# MAGIC **Purpose:** Knowledge section marker
# MAGIC
# MAGIC **What it does:** Introduces the indexing stage.
# MAGIC
# MAGIC **Why it exists:** Separates ingestion/indexing from tools and routing.
# MAGIC
# MAGIC **Cell content focus:** Part 1 — Knowledge Base Indexing
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Knowledge index
# MAGIC
# MAGIC **What it does:** Normalizes active Bronze documents into `knowledge_documents` and creates one governed chunk per document in `document_chunks`.
# MAGIC
# MAGIC **Why it exists:** Creates the retrieval corpus used by the KnowledgeAgent.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE OR REPLACE TABLE {CAT}.retail_genai.knowledge_documents AS | SELECT doc_id AS document_id, title, doc_type AS document_type, topic,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.knowledge_documents`, `retail_genai.document_chunks`, `retail_genai.knowledge_documents`
# MAGIC
# MAGIC **Reads:** `retail_bronze.knowledge_documents`, `retail_genai.knowledge_documents`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.knowledge_documents`, `retail_genai.document_chunks`, `retail_genai.knowledge_documents`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Markdown
# MAGIC
# MAGIC **Purpose:** Tools section marker
# MAGIC
# MAGIC **What it does:** Introduces safe tool implementations.
# MAGIC
# MAGIC **Why it exists:** Clarifies the agent is tool-driven rather than free-form.
# MAGIC
# MAGIC **Cell content focus:** Part 2 — Tool Implementations
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 07 — Code
# MAGIC
# MAGIC **Purpose:** Six governed tools
# MAGIC
# MAGIC **What it does:** Implements read-only SQL, lexical retrieval, whitelisted metrics, anomaly/pipeline alerts, schema health, and executive report functions.
# MAGIC
# MAGIC **Why it exists:** Each capability has constrained access to known tables and avoids raw arbitrary mutation.
# MAGIC
# MAGIC **Key code signature:** `class SQLTool: | """Read-only SQL execution. Allows SELECT/WITH only and caps returned rows.""" | def run(self, query):`
# MAGIC
# MAGIC **Project objects referenced:** `retail_genai.document_chunks`, `retail_gold.data_quality_kpi`, `retail_ml.anomaly_scores`, `retail_ml.churn_predictions`, `retail_monitoring.anomaly_events`, `retail_monitoring.pipeline_runs`, `retail_silver.dim_customers`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **Reads:** `retail_gold.data_quality_kpi`, `retail_ml.anomaly_scores`, `retail_ml.churn_predictions`, `retail_monitoring.pipeline_runs`, `retail_silver.dim_customers`, `retail_silver.fact_orders`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 08 — Markdown
# MAGIC
# MAGIC **Purpose:** Safety section marker
# MAGIC
# MAGIC **What it does:** Introduces prompt-injection protection.
# MAGIC
# MAGIC **Why it exists:** Shows safety is applied before orchestration.
# MAGIC
# MAGIC **Cell content focus:** Part 2b — Phase 13: Prompt Injection Safety Filter
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 09 — Code
# MAGIC
# MAGIC **Purpose:** Safety filter
# MAGIC
# MAGIC **What it does:** Defines injection, sensitive-information, and destructive-operation patterns; provides helper checks and a small validation suite.
# MAGIC
# MAGIC **Why it exists:** Blocks obvious jailbreak/credential/destructive requests in the demo before any tool executes.
# MAGIC
# MAGIC **Key code signature:** `INJECTION_PATTERNS = [ | "ignore previous", | "ignore instructions",`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 10 — Markdown
# MAGIC
# MAGIC **Purpose:** Agent-definition marker
# MAGIC
# MAGIC **What it does:** Introduces specialist agents.
# MAGIC
# MAGIC **Why it exists:** Separates tool primitives from domain behavior.
# MAGIC
# MAGIC **Cell content focus:** Part 3 — Agent Definitions
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 11 — Code
# MAGIC
# MAGIC **Purpose:** Five specialist agents
# MAGIC
# MAGIC **What it does:** Defines analytics, knowledge, quality, anomaly, and operations agents; each combines only the tools needed for its domain.
# MAGIC
# MAGIC **Why it exists:** Specialization makes routing/evaluation understandable and limits tool scope.
# MAGIC
# MAGIC **Key code signature:** `class AnalyticsAgent: | name = "analytics_agent" | def handle(self, query):`
# MAGIC
# MAGIC **Project objects referenced:** `retail_quality.quality_results`
# MAGIC
# MAGIC **Reads:** `retail_quality.quality_results`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 12 — Markdown
# MAGIC
# MAGIC **Purpose:** Orchestrator marker
# MAGIC
# MAGIC **What it does:** Introduces routing and integrated safety.
# MAGIC
# MAGIC **Why it exists:** Frames the layer that decides which agent receives a query.
# MAGIC
# MAGIC **Cell content focus:** Part 4 — Agent Orchestrator with Safety Integration
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 13 — Code
# MAGIC
# MAGIC **Purpose:** Agent orchestrator
# MAGIC
# MAGIC **What it does:** Checks safety, routes with word/phrase-aware matching, calls the selected agent, and returns route/status/latency/timestamp metadata.
# MAGIC
# MAGIC **Why it exists:** Prevents substring-routing bugs and makes every response measurable.
# MAGIC
# MAGIC **Key code signature:** `class AgentOrchestrator: | ROUTING = { | "quality": ["quality","rule","rules","validation","pass","fail","dq","score","completeness"],`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 14 — Markdown
# MAGIC
# MAGIC **Purpose:** Evaluation marker
# MAGIC
# MAGIC **What it does:** Explains that the following tests measure a small deterministic route/support set, not general AI accuracy.
# MAGIC
# MAGIC **Why it exists:** Keeps evaluation claims appropriately scoped.
# MAGIC
# MAGIC **Cell content focus:** Part 5 — Evaluation and Interaction Logging Runs a small deterministic routing/source-support test suite and stores both interactions and evaluation results. This is a demo evaluation set, not a claim of general LLM accuracy.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 15 — Code
# MAGIC
# MAGIC **Purpose:** Evaluation and interaction tables
# MAGIC
# MAGIC **What it does:** Runs 10 expected-route/safety cases, determines whether the response is supported, writes `agent_interactions` and `evaluation_results`, and reports routing correctness.
# MAGIC
# MAGIC **Why it exists:** Provides measurable agent behavior that the dashboard can query.
# MAGIC
# MAGIC **Key code signature:** `TEST_CASES = [ | ("analytics", "What is total revenue?", "analytics"), | ("analytics", "How many active customers do we have?", "analytics"),`
# MAGIC
# MAGIC **Project objects referenced:** `retail_genai.agent_interactions`, `retail_genai.evaluation_results`
# MAGIC
# MAGIC **Writes/updates:** `retail_genai.agent_interactions`, `retail_genai.evaluation_results`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 16 — Markdown
# MAGIC
# MAGIC **Purpose:** Summary marker
# MAGIC
# MAGIC **What it does:** Introduces the completion summary.
# MAGIC
# MAGIC **Why it exists:** Separates implementation from final readiness output.
# MAGIC
# MAGIC **Cell content focus:** Summary
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `05_genai_agent` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 17 — Code
# MAGIC
# MAGIC **Purpose:** Completion output
# MAGIC
# MAGIC **What it does:** Prints knowledge-chunk/tool/agent readiness counts and closes the notebook.
# MAGIC
# MAGIC **Why it exists:** Provides a simple visual confirmation for manual demos.
# MAGIC
# MAGIC **Key code signature:** `print("\n" + "="*65) | print("GENAI & AGENTIC SYSTEM COMPLETE") | print("="*65)`
# MAGIC
# MAGIC **Project objects referenced:** `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **Writes/updates:** `retail_monitoring.pipeline_runs`
# MAGIC
# MAGIC **How it connects:** This closes `05_genai_agent` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `06B_lakeflow_pipeline_source`
# MAGIC
# MAGIC **Cells:** 5
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Managed pipeline contract
# MAGIC
# MAGIC **What it does:** Explains that this notebook is evaluated by Lakeflow and is not a normal manual notebook.
# MAGIC
# MAGIC **Why it exists:** Prevents direct execution outside pipeline context.
# MAGIC
# MAGIC **Cell content focus:** 06B · Lakeflow Managed Realtime Pipeline Source **Do not run this notebook as a normal notebook.** It is source code for a Lakeflow pipeline and imports `pyspark.pipelines`, which is available only in pipeline context. The companion DAB deploys it as a **serverless** pipeline. It reads the same Unity Catalog landing folder used by `06_realtime_streaming`, creates a streaming Bronze table with expectations, a streaming Silver table, and a materialized Gold minute-level view. The bundle keeps the 
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06B_lakeflow_pipeline_source` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Pipeline configuration
# MAGIC
# MAGIC **What it does:** Imports `pyspark.pipelines`, Spark functions/types, resolves the source catalog from pipeline configuration, and defines the governed Volume landing path/schema.
# MAGIC
# MAGIC **Why it exists:** Decouples the pipeline from a hard-coded catalog and prepares Auto Loader schema.
# MAGIC
# MAGIC **Key code signature:** `from pyspark import pipelines as dp | from pyspark.sql import functions as F, types as T | CAT = spark.conf.get("retail.source_catalog", "workspace")`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06B_lakeflow_pipeline_source` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Bronze streaming table
# MAGIC
# MAGIC **What it does:** Uses a `@dp.table` definition to ingest JSON files from the governed Volume through Auto Loader into `orders_bronze_stream`.
# MAGIC
# MAGIC **Why it exists:** Demonstrates the managed serverless streaming ingestion pattern.
# MAGIC
# MAGIC **Key code signature:** `@dp.table( | name="orders_bronze_stream", | comment="Free Edition demo order events incrementally ingested from a Unity Catalog Volume",`
# MAGIC
# MAGIC **Important mechanisms:** Structured Streaming source, Lakeflow streaming table
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06B_lakeflow_pipeline_source` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Silver streaming table
# MAGIC
# MAGIC **What it does:** Creates a typed/derived pipeline table from Bronze events and computes net revenue/order semantics.
# MAGIC
# MAGIC **Why it exists:** Separates raw ingestion from trusted realtime records.
# MAGIC
# MAGIC **Key code signature:** `@dp.table( | name="orders_silver_stream", | comment="Typed, quality-filtered realtime order facts",`
# MAGIC
# MAGIC **Important mechanisms:** Structured Streaming source, Lakeflow streaming table
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06B_lakeflow_pipeline_source` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Minute materialized view
# MAGIC
# MAGIC **What it does:** Aggregates Silver streaming orders by minute and channel into `live_sales_minute_pipeline`.
# MAGIC
# MAGIC **Why it exists:** Provides a managed realtime business KPI output for the dashboard/agent.
# MAGIC
# MAGIC **Key code signature:** `@dp.materialized_view( | name="live_sales_minute_pipeline", | comment="Minute/channel realtime sales KPIs maintained by Lakeflow",`
# MAGIC
# MAGIC **Important mechanisms:** Lakeflow materialized view
# MAGIC
# MAGIC **How it connects:** This closes `06B_lakeflow_pipeline_source` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `06_realtime_streaming`
# MAGIC
# MAGIC **Cells:** 8
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** AvailableNow design
# MAGIC
# MAGIC **What it does:** Explains the bounded streaming design for Free Edition and the Bronze -> Silver -> Gold -> metrics path.
# MAGIC
# MAGIC **Why it exists:** Clearly distinguishes near-real-time incremental processing from hard real-time.
# MAGIC
# MAGIC **Cell content focus:** 06 · Free Edition Near-Real-Time Streaming — AvailableNow Unity Catalog landing files → Structured Streaming → Bronze → Silver → Gold → monitoring This notebook is designed for **Databricks Free Edition/serverless compute**. It does **not** use `processingTime` or an unbounded notebook stream. Each run:
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Streaming configuration
# MAGIC
# MAGIC **What it does:** Validates catalog, imports streaming/Delta helpers, exposes `events_per_run`, creates/validates the managed Volume, defines landing/checkpoint paths, and generates a unique run token.
# MAGIC
# MAGIC **Why it exists:** Makes each run independently identifiable while reusing a persistent checkpoint.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Realtime target tables
# MAGIC
# MAGIC **What it does:** Creates the Bronze realtime event table, Silver realtime order fact, and Gold minute KPI table if absent.
# MAGIC
# MAGIC **Why it exists:** Allows the first stream run to initialize its contract without manual DDL.
# MAGIC
# MAGIC **Key code signature:** `spark.sql(f""" | CREATE TABLE IF NOT EXISTS {CAT}.retail_bronze.realtime_order_events ( | event_id STRING, order_id STRING, customer_id STRING, store_id STRING,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.realtime_order_events`, `retail_gold.live_sales_minute`, `retail_silver.fact_orders_realtime`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.realtime_order_events`, `retail_gold.live_sales_minute`, `retail_silver.fact_orders_realtime`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Land finite source files
# MAGIC
# MAGIC **What it does:** Generates a configurable set of realistic order events and appends them as JSON files to the governed landing path.
# MAGIC
# MAGIC **Why it exists:** Simulates new source arrivals without an external Kafka/Event Hubs dependency.
# MAGIC
# MAGIC **Key code signature:** `base = spark.range(EVENTS_PER_RUN).withColumnRenamed("id", "seq") | landed = base.select( | F.concat(F.lit(f"RTEVT_{RUN_TOKEN}_"), F.lpad(F.col("seq").cast("string"), 6, "0")).alias("event_id"),`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Define streaming source
# MAGIC
# MAGIC **What it does:** Declares an explicit StructType and creates `spark.readStream` over the JSON landing path with a file limit.
# MAGIC
# MAGIC **Why it exists:** Explicit schema avoids inference overhead/instability in streaming.
# MAGIC
# MAGIC **Key code signature:** `EVENT_SCHEMA = T.StructType([ | T.StructField("event_id", T.StringType(), False), | T.StructField("order_id", T.StringType(), False),`
# MAGIC
# MAGIC **Important mechanisms:** Structured Streaming source
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Code
# MAGIC
# MAGIC **Purpose:** Idempotent `foreachBatch` processing
# MAGIC
# MAGIC **What it does:** Deduplicates event IDs, MERGEs Bronze, derives and MERGEs Silver, recalculates affected minute/channel Gold keys, MERGEs operational metrics, records failures, and unpersists cached frames.
# MAGIC
# MAGIC **Why it exists:** Provides retry-safe-ish behavior and ensures business aggregates are recomputed from deduplicated state rather than blindly incremented.
# MAGIC
# MAGIC **Key code signature:** `def process_microbatch(batch_df, batch_id): | started = time.perf_counter() | started_at = datetime.now(timezone.utc).replace(tzinfo=None)`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.realtime_order_events`, `retail_gold.live_sales_minute`, `retail_monitoring.streaming_metrics`, `retail_silver.fact_orders_realtime`
# MAGIC
# MAGIC **Reads:** `retail_silver.fact_orders_realtime`
# MAGIC
# MAGIC **Writes/updates:** `retail_bronze.realtime_order_events`, `retail_gold.live_sales_minute`, `retail_monitoring.streaming_metrics`, `retail_silver.fact_orders_realtime`
# MAGIC
# MAGIC **Important mechanisms:** Delta MERGE
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 07 — Code
# MAGIC
# MAGIC **Purpose:** Run with `Trigger.AvailableNow()`
# MAGIC
# MAGIC **What it does:** Starts the query with the persistent checkpoint, processes all newly available files, waits for completion, and exits.
# MAGIC
# MAGIC **Why it exists:** This is the serverless-safe notebook/job streaming pattern for Free Edition.
# MAGIC
# MAGIC **Key code signature:** `query = (stream_events.writeStream | .queryName(STREAM_NAME) | .foreachBatch(process_microbatch)`
# MAGIC
# MAGIC **Important mechanisms:** Structured Streaming sink, foreachBatch
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `06_realtime_streaming` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 08 — Code
# MAGIC
# MAGIC **Purpose:** Realtime result display
# MAGIC
# MAGIC **What it does:** Shows latest minute/channel business KPIs and recent micro-batch health metrics.
# MAGIC
# MAGIC **Why it exists:** Gives an immediate visual demo that incremental processing changed both business and operational tables.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT event_minute, channel, order_count, unique_customers, revenue, avg_order_value, last_updated | FROM {CAT}.retail_gold.live_sales_minute`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.live_sales_minute`, `retail_monitoring.streaming_metrics`
# MAGIC
# MAGIC **Reads:** `retail_gold.live_sales_minute`, `retail_monitoring.streaming_metrics`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** This closes `06_realtime_streaming` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `07_runtime_validation`
# MAGIC
# MAGIC **Cells:** 4
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Validation contract
# MAGIC
# MAGIC **What it does:** States the end-to-end checks and explains optional Lakeflow validation behavior.
# MAGIC
# MAGIC **Why it exists:** Makes this notebook the final acceptance gate rather than another transformation step.
# MAGIC
# MAGIC **Cell content focus:** 07 · End-to-End Runtime Validation — Free Edition E2E v4.0 Run after `00`–`06`. It verifies object existence, referential/financial consistency, data-quality execution, ML coverage, agent evaluation, AvailableNow streaming output, and the governed Unity Catalog volume. If the DAB Lakeflow pipeline has also run, its three managed realtime outputs are validated as well.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `07_runtime_validation` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Validation harness
# MAGIC
# MAGIC **What it does:** Configures catalog/run metadata and defines helpers that collect typed PASS/FAIL/SKIP check rows.
# MAGIC
# MAGIC **Why it exists:** Standardizes validation evidence into a table-ready structure.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `07_runtime_validation` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Core data/object assertions
# MAGIC
# MAGIC **What it does:** Checks required objects plus referential, financial, payment, inventory, shipment, return, future-date, and quality-engine invariants.
# MAGIC
# MAGIC **Why it exists:** Catches logical failures that syntax/static checks cannot detect.
# MAGIC
# MAGIC **Key code signature:** `expected = [ | 'retail_bronze.customers','retail_bronze.orders','retail_bronze.order_items','retail_silver.fact_orders', | 'retail_gold.customer_360','retail_gold.product_kpis','retail_quality.quality_results',`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.customers`, `retail_bronze.inventory_events`, `retail_bronze.order_items`, `retail_bronze.orders`, `retail_bronze.payments`, `retail_bronze.returns`, `retail_bronze.shipments`, `retail_quality.quality_results`
# MAGIC
# MAGIC **Reads:** `retail_bronze.customers`, `retail_bronze.inventory_events`, `retail_bronze.order_items`, `retail_bronze.orders`, `retail_bronze.payments`, `retail_bronze.returns`, `retail_bronze.shipments`, `retail_quality.quality_results`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `07_runtime_validation` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** ML/AI/streaming/Lakeflow assertions and result persistence
# MAGIC
# MAGIC **What it does:** Checks ML customer coverage, agent route evaluation, Volume presence, AvailableNow output/metrics, optionally managed pipeline objects, writes `validation_results`, logs the validation run, and displays failures.
# MAGIC
# MAGIC **Why it exists:** Provides the final workspace-specific proof that the project actually executed successfully.
# MAGIC
# MAGIC **Key code signature:** `try: | customers=scalar(f"SELECT COUNT(*) FROM {CAT}.retail_silver.dim_customers"); scored=scalar(f"SELECT COUNT(*) FROM {CAT}.retail_ml.churn_predictions") | add("ml_customer_coverage","ml","PASS" if customers==scored else "FAIL",scored,customers)`
# MAGIC
# MAGIC **Project objects referenced:** `retail_bronze.realtime_order_events`, `retail_genai.evaluation_results`, `retail_gold.live_sales_minute`, `retail_ml.churn_predictions`, `retail_monitoring.pipeline_runs`, `retail_monitoring.streaming_metrics`, `retail_monitoring.validation_results`, `retail_silver.dim_customers`
# MAGIC
# MAGIC **Reads:** `retail_bronze.realtime_order_events`, `retail_genai.evaluation_results`, `retail_gold.live_sales_minute`, `retail_ml.churn_predictions`, `retail_monitoring.streaming_metrics`, `retail_silver.dim_customers`
# MAGIC
# MAGIC **Writes/updates:** `retail_monitoring.pipeline_runs`, `retail_monitoring.validation_results`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** This closes `07_runtime_validation` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `08_DAB_DEPLOYMENT_GUIDE`
# MAGIC
# MAGIC **Cells:** 3
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** DAB boundary
# MAGIC
# MAGIC **What it does:** Explains why the companion bundle is required to deploy Jobs/pipelines/resources that the DBC cannot create.
# MAGIC
# MAGIC **Why it exists:** Prevents confusion between notebook archive and infrastructure deployment.
# MAGIC
# MAGIC **Cell content focus:** 08 · Declarative Automation Bundle (DAB) Deployment Guide The DBC and DAB serve different purposes: **DBC = notebook import**, **DAB = resource deployment**. Use the companion ZIP `retail-ai-free-edition-dab-v4.0.zip` when you want Databricks to create/update Jobs and the Lakeflow pipeline automatically.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `08_DAB_DEPLOYMENT_GUIDE` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Markdown
# MAGIC
# MAGIC **Purpose:** Deploy commands
# MAGIC
# MAGIC **What it does:** Documents bundle validate/deploy/run commands and catalog override usage.
# MAGIC
# MAGIC **Why it exists:** Provides a reproducible deployment procedure.
# MAGIC
# MAGIC **Cell content focus:** Deploy From a machine with a current Databricks CLI, authenticate to your Free Edition workspace, unzip the bundle, then run: ```bash
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `08_DAB_DEPLOYMENT_GUIDE` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Markdown
# MAGIC
# MAGIC **Purpose:** Existing pipeline binding
# MAGIC
# MAGIC **What it does:** Explains how an already-created pipeline can be bound to the bundle instead of duplicated.
# MAGIC
# MAGIC **Why it exists:** Supports users who created resources manually before adopting DABs.
# MAGIC
# MAGIC **Cell content focus:** Existing pipeline If you already created a Lakeflow pipeline and want the bundle to manage that same resource instead of creating a second one, use Databricks bundle resource binding after deployment/generation. Keep only one active realtime pipeline path at a time to avoid two consumers writing competing demo outputs. Recommended submission/demo
# MAGIC
# MAGIC **How it connects:** This closes `08_DAB_DEPLOYMENT_GUIDE` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `AGENT_CHAT`
# MAGIC
# MAGIC **Cells:** 6
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Interactive chat overview
# MAGIC
# MAGIC **What it does:** Explains the notebook is the presentation-facing interface to the governed agent tools.
# MAGIC
# MAGIC **Why it exists:** Provides a simple demo surface separate from the implementation notebook.
# MAGIC
# MAGIC **Cell content focus:** RetailHub AI Agent — Interactive Chat **No app needed.** Type your question in the widget below and run the cell. **Example questions:**
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `AGENT_CHAT` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Code
# MAGIC
# MAGIC **Purpose:** Interactive agent implementation
# MAGIC
# MAGIC **What it does:** Defines safe retrieval/KPI/customer/quality/pipeline/ML/realtime helper functions, safety patterns, prioritized routes, and `ask_agent()`.
# MAGIC
# MAGIC **Why it exists:** Keeps interactive queries deterministic and routed to governed data products; realtime keywords are prioritized before generic sales keywords.
# MAGIC
# MAGIC **Key code signature:** `from pyspark.sql import functions as F | import re | try:`
# MAGIC
# MAGIC **Project objects referenced:** `retail_genai.document_chunks`, `retail_gold.customer_360`, `retail_gold.daily_sales_summary`, `retail_gold.executive_kpi`, `retail_gold.inventory_health`, `retail_gold.live_sales_minute`, `retail_ml.churn_predictions`, `retail_ml.customer_segments`, `retail_ml.demand_forecast`, `retail_monitoring.anomaly_events`, `retail_monitoring.pipeline_runs`, `retail_quality.quality_summary`, `retail_realtime.live_sales_minute_pipeline`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_360`, `retail_gold.daily_sales_summary`, `retail_gold.executive_kpi`, `retail_gold.inventory_health`, `retail_ml.churn_predictions`, `retail_ml.customer_segments`, `retail_ml.demand_forecast`, `retail_monitoring.anomaly_events`, `retail_monitoring.pipeline_runs`, `retail_quality.quality_summary`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `AGENT_CHAT` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Markdown
# MAGIC
# MAGIC **Purpose:** User instructions
# MAGIC
# MAGIC **What it does:** Tells the presenter how to submit a widget question.
# MAGIC
# MAGIC **Why it exists:** Makes the notebook self-explanatory during a demo.
# MAGIC
# MAGIC **Cell content focus:** --- Ask the Agent **Step 1:** Enter your question in the widget above
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `AGENT_CHAT` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Code
# MAGIC
# MAGIC **Purpose:** Question widget
# MAGIC
# MAGIC **What it does:** Creates the `question` text widget with a return-policy default.
# MAGIC
# MAGIC **Why it exists:** Allows non-technical users to change prompts without editing code.
# MAGIC
# MAGIC **Key code signature:** `dbutils.widgets.text("question", "What is the return policy?", "Your Question")`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `AGENT_CHAT` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Execute and render response
# MAGIC
# MAGIC **What it does:** Runs `ask_agent`, HTML-escapes the user question, renders a status/tool card, and prints the grounded answer.
# MAGIC
# MAGIC **Why it exists:** Provides a readable UI while avoiding the earlier module-shadowing issue by importing `html as html_lib`.
# MAGIC
# MAGIC **Key code signature:** `import html as html_lib | question = dbutils.widgets.get("question") | if not question.strip():`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `AGENT_CHAT` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Markdown
# MAGIC
# MAGIC **Purpose:** Example prompts
# MAGIC
# MAGIC **What it does:** Lists policy, sales, KPI, inventory, churn, anomaly, quality, pipeline, realtime, ML, customer, and safety-test questions.
# MAGIC
# MAGIC **Why it exists:** Gives the presenter a stable demo script.
# MAGIC
# MAGIC **Cell content focus:** --- Try These Example Questions Copy any question above into the widget and re-run:
# MAGIC
# MAGIC **How it connects:** This closes `AGENT_CHAT` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `NOTEBOOK_WALKTHROUGH`
# MAGIC
# MAGIC **Cells:** 1
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Notebook walkthrough
# MAGIC
# MAGIC **What it does:** Provides implementation-aligned descriptions of the main notebook sequence and outputs.
# MAGIC
# MAGIC **Why it exists:** Useful for navigation and high-level code reading.
# MAGIC
# MAGIC **Cell content focus:** Notebook Walkthrough — Corrected Implementation Run order **Batch validation path:** `00_setup` → `01_bronze_ingestion` → `02_silver_gold_pipeline` → `03_data_quality_checks` → `04_ml_training` → `05_genai_agent` → `07_runtime_validation`.
# MAGIC
# MAGIC **How it connects:** This closes `NOTEBOOK_WALKTHROUGH` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `OPTIMIZED_PROJECT_SUMMARY`
# MAGIC
# MAGIC **Cells:** 1
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Earlier optimization summary
# MAGIC
# MAGIC **What it does:** Documents the corrected architecture, major fixes, realtime approach, and validation caveats.
# MAGIC
# MAGIC **Why it exists:** Provides project history/context; the new detailed summary is more exhaustive.
# MAGIC
# MAGIC **Cell content focus:** Retail AI — Final Fixed Project Summary Final architecture The project now implements a consistent Databricks lakehouse demonstration across batch and near-real-time paths: **Bronze → Silver → Gold → ML / GenAI / Dashboard**, with metadata-driven Data Quality and operational Monitoring as cross-cutting layers.
# MAGIC
# MAGIC **How it connects:** This closes `OPTIMIZED_PROJECT_SUMMARY` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `PLATFORM_GUIDE`
# MAGIC
# MAGIC **Cells:** 1
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Platform guide
# MAGIC
# MAGIC **What it does:** Explains how the major data, ML, AI, monitoring, and realtime components fit together.
# MAGIC
# MAGIC **Why it exists:** Acts as a platform-oriented reference rather than cell-level documentation.
# MAGIC
# MAGIC **Cell content focus:** Retail AI Platform Guide — Implementation-Aligned Architecture **Bronze → Silver → Gold → ML / GenAI / Dashboards**, with Data Quality and Monitoring as cross-cutting layers and a separate near-real-time stream feeding Bronze/Silver/Gold live tables.
# MAGIC
# MAGIC **How it connects:** This closes `PLATFORM_GUIDE` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `PRESENTATION_TEXT`
# MAGIC
# MAGIC **Cells:** 1
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Presentation script
# MAGIC
# MAGIC **What it does:** Contains the previous slide-by-slide presentation narrative.
# MAGIC
# MAGIC **Why it exists:** Provides a usable baseline; the new WHAT/WHY/HOW presentation expands it further.
# MAGIC
# MAGIC **Cell content focus:** RetailHub AI — Detailed Presentation Text Slide 1 — Real-Time Intelligent Retail Lakehouse **On-slide text**
# MAGIC
# MAGIC **How it connects:** This closes `PRESENTATION_TEXT` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `RESULTS_DASHBOARD`
# MAGIC
# MAGIC **Cells:** 33
# MAGIC
# MAGIC ## Cell 01 — Code
# MAGIC
# MAGIC **Purpose:** Dashboard context
# MAGIC
# MAGIC **What it does:** Validates catalog and prepares the notebook to query project outputs.
# MAGIC
# MAGIC **Why it exists:** Keeps all dashboard SQL portable to the selected catalog.
# MAGIC
# MAGIC **Key code signature:** `import re | try: | _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]`
# MAGIC
# MAGIC **Project objects referenced:** `retail_quality.quality_summary`
# MAGIC
# MAGIC **Reads:** `retail_quality.quality_summary`
# MAGIC
# MAGIC **Important mechanisms:** Databricks widgets, Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 02 — Markdown
# MAGIC
# MAGIC **Purpose:** Dashboard usage note
# MAGIC
# MAGIC **What it does:** Explains historical + incremental realtime behavior and the optional managed pipeline path.
# MAGIC
# MAGIC **Why it exists:** Prevents users from expecting live tables before running the upstream pipeline.
# MAGIC
# MAGIC **Cell content focus:** RetailHub Platform — Results Dashboard Batch history + near-real-time streaming views The optimized dashboard references tables created by this project. Run the numbered pipeline notebooks first. For Free Edition live data, run `06_realtime_streaming`; each invocation processes a new AvailableNow increment and exits. Re-run it, then refresh the live cells. The DAB also provides a managed Lakeflow pipeline path.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 03 — Code
# MAGIC
# MAGIC **Purpose:** Context confirmation
# MAGIC
# MAGIC **What it does:** Prints the resolved catalog.
# MAGIC
# MAGIC **Why it exists:** Simple sanity check before running dashboard cells.
# MAGIC
# MAGIC **Key code signature:** `print(f"Dashboard context ready: {CAT}")`
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 04 — Markdown
# MAGIC
# MAGIC **Purpose:** Executive KPI heading
# MAGIC
# MAGIC **What it does:** Labels the next visualization section.
# MAGIC
# MAGIC **Why it exists:** Separates dashboard topics visually.
# MAGIC
# MAGIC **Cell content focus:** Executive KPI
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 05 — Code
# MAGIC
# MAGIC **Purpose:** Executive KPI query
# MAGIC
# MAGIC **What it does:** Displays the one-row Gold executive snapshot with order attempts vs recognized orders, revenue, AOV, returns, products, and open tickets.
# MAGIC
# MAGIC **Why it exists:** Provides the top-level business health view.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT | CAST(total_customers AS INT) as total_customers,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.executive_kpi`
# MAGIC
# MAGIC **Reads:** `retail_gold.executive_kpi`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 06 — Markdown
# MAGIC
# MAGIC **Purpose:** Revenue heading
# MAGIC
# MAGIC **What it does:** Labels the last-30-day revenue section.
# MAGIC
# MAGIC **Why it exists:** Provides narrative structure.
# MAGIC
# MAGIC **Cell content focus:** Revenue by Channel (Last 30 Days)
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 07 — Code
# MAGIC
# MAGIC **Purpose:** Revenue by channel
# MAGIC
# MAGIC **What it does:** Queries daily sales for the last 30 days ordered by date/revenue.
# MAGIC
# MAGIC **Why it exists:** Shows recent trends using current-date-anchored source data.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT order_date, channel, | total_orders, ROUND(total_revenue,2) as revenue, ROUND(avg_order_value,2) as aov`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.daily_sales_summary`
# MAGIC
# MAGIC **Reads:** `retail_gold.daily_sales_summary`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 08 — Markdown
# MAGIC
# MAGIC **Purpose:** Customer 360 heading
# MAGIC
# MAGIC **What it does:** Labels the customer value view.
# MAGIC
# MAGIC **Why it exists:** Provides narrative structure.
# MAGIC
# MAGIC **Cell content focus:** Customer 360 — Top 20 by Revenue
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 09 — Code
# MAGIC
# MAGIC **Purpose:** Top Customer 360
# MAGIC
# MAGIC **What it does:** Displays highest-revenue customers with segment, recency, churn-risk and CLV fields.
# MAGIC
# MAGIC **Why it exists:** Connects business value, retention risk, and lifetime value.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT customer_id, full_name, customer_segment, country, | total_orders, ROUND(total_revenue,2) as lifetime_revenue,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_360`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_360`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 10 — Markdown
# MAGIC
# MAGIC **Purpose:** Churn heading
# MAGIC
# MAGIC **What it does:** Labels ML risk distribution.
# MAGIC
# MAGIC **Why it exists:** Separates supervised ML output.
# MAGIC
# MAGIC **Cell content focus:** ML Model Results — Churn Risk Distribution
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 11 — Code
# MAGIC
# MAGIC **Purpose:** Churn distribution
# MAGIC
# MAGIC **What it does:** Buckets probabilities into High/Medium/Low and summarizes counts/average probability.
# MAGIC
# MAGIC **Why it exists:** Turns per-customer model scores into an executive distribution.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT | CASE WHEN churn_probability >= 0.7 THEN 'High (>=70%)'`
# MAGIC
# MAGIC **Project objects referenced:** `retail_ml.churn_predictions`
# MAGIC
# MAGIC **Reads:** `retail_ml.churn_predictions`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 12 — Markdown
# MAGIC
# MAGIC **Purpose:** Segments heading
# MAGIC
# MAGIC **What it does:** Labels segmentation output.
# MAGIC
# MAGIC **Why it exists:** Separates unsupervised ML output.
# MAGIC
# MAGIC **Cell content focus:** ML — Customer Segments
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 13 — Code
# MAGIC
# MAGIC **Purpose:** Segment profile
# MAGIC
# MAGIC **What it does:** Joins segment labels to customer KPIs and summarizes population/revenue/orders/recency by segment.
# MAGIC
# MAGIC **Why it exists:** Explains what the learned clusters mean in business terms.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT s.segment_label, | COUNT(*) as customers,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_kpis`, `retail_ml.customer_segments`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_kpis`, `retail_ml.customer_segments`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 14 — Markdown
# MAGIC
# MAGIC **Purpose:** Anomaly heading
# MAGIC
# MAGIC **What it does:** Labels anomaly output.
# MAGIC
# MAGIC **Why it exists:** Separates anomaly analysis.
# MAGIC
# MAGIC **Cell content focus:** Anomaly Detection — Top Flagged Customers
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 15 — Code
# MAGIC
# MAGIC **Purpose:** Top anomalies
# MAGIC
# MAGIC **What it does:** Joins anomaly events to Customer 360 and shows top flagged customers.
# MAGIC
# MAGIC **Why it exists:** Adds customer context to technical anomaly scores.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT a.entity_id as customer_id, c.customer_segment, c.country, | ROUND(a.anomaly_score,4) as anomaly_score,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.customer_360`, `retail_monitoring.anomaly_events`
# MAGIC
# MAGIC **Reads:** `retail_gold.customer_360`, `retail_monitoring.anomaly_events`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 16 — Markdown
# MAGIC
# MAGIC **Purpose:** Demand heading
# MAGIC
# MAGIC **What it does:** Labels demand baseline.
# MAGIC
# MAGIC **Why it exists:** Separates planning output.
# MAGIC
# MAGIC **Cell content focus:** Demand Baseline — Top 20 Products
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 17 — Code
# MAGIC
# MAGIC **Purpose:** Top demand baseline
# MAGIC
# MAGIC **What it does:** Joins baseline units to product details and derives a rough baseline revenue estimate.
# MAGIC
# MAGIC **Why it exists:** Provides an interpretable planning view while retaining the baseline label.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT d.product_id, p.product_name, p.category_name, p.price, | d.predicted_units, ROUND(d.stability_score*100,1) as stability_score_pct,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_ml.demand_forecast`, `retail_silver.dim_products`
# MAGIC
# MAGIC **Reads:** `retail_ml.demand_forecast`, `retail_silver.dim_products`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 18 — Markdown
# MAGIC
# MAGIC **Purpose:** Inventory heading
# MAGIC
# MAGIC **What it does:** Labels inventory health.
# MAGIC
# MAGIC **Why it exists:** Separates operations output.
# MAGIC
# MAGIC **Cell content focus:** Inventory Health
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 19 — Code
# MAGIC
# MAGIC **Purpose:** Inventory health
# MAGIC
# MAGIC **What it does:** Groups product/warehouse combinations by stock status and summarizes units/reorder need.
# MAGIC
# MAGIC **Why it exists:** Shows where operational attention is required.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT stock_status, COUNT(*) as product_warehouse_combos, | SUM(current_stock) as total_units,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.inventory_health`
# MAGIC
# MAGIC **Reads:** `retail_gold.inventory_health`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 20 — Markdown
# MAGIC
# MAGIC **Purpose:** Quality heading
# MAGIC
# MAGIC **What it does:** Labels quality results.
# MAGIC
# MAGIC **Why it exists:** Separates data trust metrics.
# MAGIC
# MAGIC **Cell content focus:** Data Quality — All Rules
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 21 — Code
# MAGIC
# MAGIC **Purpose:** All quality rules
# MAGIC
# MAGIC **What it does:** Joins latest rule results to metadata and orders by severity/status.
# MAGIC
# MAGIC **Why it exists:** Lets reviewers inspect actual rule evidence, not only an overall score.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT r.rule_id, r.rule_type, r.dataset, r.field, r.severity, | r.description,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_quality.quality_results`, `retail_quality.quality_rules`
# MAGIC
# MAGIC **Reads:** `retail_quality.quality_results`, `retail_quality.quality_rules`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 22 — Markdown
# MAGIC
# MAGIC **Purpose:** Agent evaluation heading
# MAGIC
# MAGIC **What it does:** Labels measured test-set results.
# MAGIC
# MAGIC **Why it exists:** Avoids presenting anecdotal AI demos as proof.
# MAGIC
# MAGIC **Cell content focus:** Agent Evaluation — Measured Test Set
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 23 — Code
# MAGIC
# MAGIC **Purpose:** Agent evaluation
# MAGIC
# MAGIC **What it does:** Summarizes route correctness, support, latency, and accuracy by question type.
# MAGIC
# MAGIC **Why it exists:** Makes the agent measurable.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT question_type, COUNT(*) as questions, | SUM(CASE WHEN tool_correct THEN 1 ELSE 0 END) as correctly_routed,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_genai.evaluation_results`
# MAGIC
# MAGIC **Reads:** `retail_genai.evaluation_results`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 24 — Markdown
# MAGIC
# MAGIC **Purpose:** Interactions heading
# MAGIC
# MAGIC **What it does:** Labels interaction history.
# MAGIC
# MAGIC **Why it exists:** Separates operational AI logs.
# MAGIC
# MAGIC **Cell content focus:** Agent Interactions Log
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 25 — Code
# MAGIC
# MAGIC **Purpose:** Agent interactions
# MAGIC
# MAGIC **What it does:** Shows recent query, route/tool, status, latency, response preview, and timestamp.
# MAGIC
# MAGIC **Why it exists:** Provides traceability for assistant behavior.
# MAGIC
# MAGIC **Key code signature:** `display(spark.sql(f""" | SELECT interaction_id, session_id, query, agent_name, tool_used, status, | ROUND(latency_ms,1) as latency_ms,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_genai.agent_interactions`
# MAGIC
# MAGIC **Reads:** `retail_genai.agent_interactions`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 26 — Markdown
# MAGIC
# MAGIC **Purpose:** Realtime heading
# MAGIC
# MAGIC **What it does:** Labels AvailableNow live-sales output.
# MAGIC
# MAGIC **Why it exists:** Separates historical batch analytics from incremental results.
# MAGIC
# MAGIC **Cell content focus:** Real-Time Sales — Latest Minutes
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 27 — Code
# MAGIC
# MAGIC **Purpose:** Latest live sales
# MAGIC
# MAGIC **What it does:** Queries `live_sales_minute` and gracefully prints instructions if the realtime notebook has not run.
# MAGIC
# MAGIC **Why it exists:** Keeps dashboard usable before/after realtime initialization.
# MAGIC
# MAGIC **Key code signature:** `try: | display(spark.sql(f""" | SELECT event_minute, channel, order_count, unique_customers,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_gold.live_sales_minute`
# MAGIC
# MAGIC **Reads:** `retail_gold.live_sales_minute`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 28 — Markdown
# MAGIC
# MAGIC **Purpose:** Streaming-health heading
# MAGIC
# MAGIC **What it does:** Labels stream observability.
# MAGIC
# MAGIC **Why it exists:** Pairs business freshness with engineering health.
# MAGIC
# MAGIC **Cell content focus:** Streaming Health & Micro-batch Latency
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 29 — Code
# MAGIC
# MAGIC **Purpose:** Streaming metrics
# MAGIC
# MAGIC **What it does:** Displays micro-batch rows, processing time, lag, status, and derived rows/sec with graceful fallback.
# MAGIC
# MAGIC **Why it exists:** Shows whether the data is fresh and how the incremental processor behaved.
# MAGIC
# MAGIC **Key code signature:** `try: | display(spark.sql(f""" | SELECT stream_name, microbatch_id, input_rows, bronze_rows, silver_rows,`
# MAGIC
# MAGIC **Project objects referenced:** `retail_monitoring.streaming_metrics`
# MAGIC
# MAGIC **Reads:** `retail_monitoring.streaming_metrics`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 30 — Markdown
# MAGIC
# MAGIC **Purpose:** Interactive usage note
# MAGIC
# MAGIC **What it does:** Points users to AGENT_CHAT, streaming reruns, and SQL Editor.
# MAGIC
# MAGIC **Why it exists:** Connects the dashboard to the broader project workflow.
# MAGIC
# MAGIC **Cell content focus:** --- How to use the project interactively - **AGENT_CHAT**: ask policy, KPI, quality, anomaly, customer, and live-stream questions.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 31 — Markdown
# MAGIC
# MAGIC **Purpose:** Architecture recap
# MAGIC
# MAGIC **What it does:** Summarizes the implemented correctness/ML/GenAI/streaming/runtime-validation improvements.
# MAGIC
# MAGIC **Why it exists:** Provides reviewers with an implementation-aligned architecture statement.
# MAGIC
# MAGIC **Cell content focus:** --- Optimized Runtime Architecture **Implemented in this archive:**
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 32 — Markdown
# MAGIC
# MAGIC **Purpose:** Managed Lakeflow heading
# MAGIC
# MAGIC **What it does:** Introduces the optional DAB-managed realtime view.
# MAGIC
# MAGIC **Why it exists:** Distinguishes pipeline-managed output from notebook AvailableNow output.
# MAGIC
# MAGIC **Cell content focus:** --- Managed Lakeflow Realtime Path (optional DAB deployment) If the companion DAB pipeline has run, this shows its independently managed streaming/materialized outputs.
# MAGIC
# MAGIC **How it connects:** Its output/context is consumed by later cells in `RESULTS_DASHBOARD` or by downstream notebooks in the numbered pipeline.
# MAGIC
# MAGIC ## Cell 33 — Code
# MAGIC
# MAGIC **Purpose:** Managed Lakeflow output
# MAGIC
# MAGIC **What it does:** Displays the pipeline materialized view if it exists or tells the user how to create it.
# MAGIC
# MAGIC **Why it exists:** Lets one dashboard demonstrate both realtime execution modes.
# MAGIC
# MAGIC **Key code signature:** `try: | display(spark.sql(f""" | SELECT event_minute, channel, order_count, unique_customers, revenue, avg_order_value, last_updated`
# MAGIC
# MAGIC **Project objects referenced:** `retail_realtime.live_sales_minute_pipeline`
# MAGIC
# MAGIC **Reads:** `retail_realtime.live_sales_minute_pipeline`
# MAGIC
# MAGIC **Important mechanisms:** Databricks display
# MAGIC
# MAGIC **How it connects:** This closes `RESULTS_DASHBOARD` and leaves its tables/metadata/documentation available to downstream notebooks or the user.
# MAGIC
# MAGIC # Notebook: `Summary_Each_Step`
# MAGIC
# MAGIC **Cells:** 1
# MAGIC
# MAGIC ## Cell 01 — Markdown
# MAGIC
# MAGIC **Purpose:** Condensed implementation summary
# MAGIC
# MAGIC **What it does:** Provides a compact notebook-by-notebook description of the corrected platform.
# MAGIC
# MAGIC **Why it exists:** Useful as a short revision sheet before a presentation or viva.
# MAGIC
# MAGIC **Cell content focus:** Retail AI Platform — Corrected Step Summary What is actually implemented A Databricks retail lakehouse demonstration with Bronze/Silver/Gold, 36 metadata-driven quality rules, MLflow-tracked sklearn models, a grounded deterministic agent, batch monitoring, and near-real-time Structured Streaming.
# MAGIC
# MAGIC **How it connects:** This closes `Summary_Each_Step` and leaves its tables/metadata/documentation available to downstream notebooks or the user.

# COMMAND ----------

# MAGIC %md
# MAGIC ## v4.2 Industry-Readiness Update
# MAGIC The Free Edition project now standardizes Spark timestamps to UTC, records duration/project version/environment in pipeline monitoring, blocks critical/error data-quality failures, captures DQ failure samples, and uses clearer semantic names including `fact_inventory_daily_movement`, `baseline_3yr_clv`, and `recency_risk`.
# MAGIC
# MAGIC ML training compares multiple churn classifiers, removes the direct recency leakage feature from churn training, evaluates K-Means cluster counts using silhouette score, and bases anomaly severity on anomaly score. The streaming path remains Free Edition-safe with `Trigger.AvailableNow()`, insert-only Bronze event handling, event-time ordering in Silver, and operational telemetry.
# MAGIC
# MAGIC Runtime validation now includes cross-layer reconciliation and blocking failures for core integrity checks. These are production-style patterns demonstrated within Databricks Free Edition constraints; real production deployment still requires real source systems, CI/CD, environment separation, permissions, secrets, SLOs, and real-data model validation.
