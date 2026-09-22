# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 - Silver & Gold · Curated Analytics Layer
# MAGIC Transforms Bronze into typed Silver facts/dimensions and business-facing Gold products. Revenue is refund-aware, cancelled orders recognize zero revenue, product KPIs aggregate sales/reviews/refunds independently to avoid many-to-many inflation, and dates are explicitly typed.
# MAGIC 
# MAGIC The batch Gold layer is complemented by `06_realtime_streaming`, which maintains separate live minute-level KPIs without rebuilding historical tables.

# COMMAND ----------

# DBTITLE 1,Silver/Gold Setup
import re
try:
    _default_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
except Exception:
    _default_catalog = "workspace"
dbutils.widgets.text("catalog", _default_catalog, "Target catalog")
CAT = dbutils.widgets.get("catalog").strip() or _default_catalog
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CAT):
    raise ValueError("Catalog must contain only letters, numbers, and underscores and cannot start with a number.")
PROJECT_VERSION = "4.0-free-edition-e2e-2026-09-21"
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row

spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN_ID = str(uuid.uuid4())[:8]
_silver_start = datetime.now(timezone.utc).replace(tzinfo=None)
NOW = _silver_start
print(f"Run: {RUN_ID}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_date AS
SELECT cast(date_format(d,'yyyyMMdd') as int) as date_key, d as full_date,
       year(d) as year, quarter(d) as quarter, month(d) as month,
       date_format(d,'MMMM') as month_name, weekofyear(d) as week_of_year,
       dayofmonth(d) as day_of_month, dayofweek(d) as day_of_week,
       date_format(d,'EEEE') as day_name,
       CASE WHEN dayofweek(d) IN (1,7) THEN true ELSE false END as is_weekend,
       CASE WHEN month(d) IN (12,1,2) THEN 'Winter' WHEN month(d) IN (3,4,5) THEN 'Spring'
            WHEN month(d) IN (6,7,8) THEN 'Summer' ELSE 'Autumn' END as season
FROM (SELECT explode(sequence(add_months(current_date(),-120), date_add(current_date(),730), interval 1 day)) as d)
""")
print(f"dim_date: {spark.table(f'{CAT}.retail_silver.dim_date').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_customers AS
SELECT customer_id, first_name, last_name, concat(first_name,' ',last_name) as full_name,
       lower(trim(email)) as email_normalized, phone, to_date(date_of_birth) as date_of_birth,
       customer_segment, to_date(registration_date) as registration_date, country, city, status,
       loyalty_points, current_date() as valid_from, date('9999-12-31') as valid_to,
       true as is_current, _batch_id, _run_id
FROM {CAT}.retail_bronze.customers
""")
print(f"dim_customers: {spark.table(f'{CAT}.retail_silver.dim_customers').count()}")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_products AS SELECT p.product_id, p.name as product_name, p.category_id, c.name as category_name, p.supplier_id, p.price, p.cost, p.price-p.cost as gross_margin, round((p.price-p.cost)/p.price*100,2) as margin_pct, p.sku, p.weight_kg, p.status FROM {CAT}.retail_bronze.products p LEFT JOIN {CAT}.retail_bronze.categories c ON p.category_id=c.category_id")
print(f"dim_products: {spark.table(f'{CAT}.retail_silver.dim_products').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_stores AS
SELECT store_id, name as store_name, city, state, country, store_type, size_sqft,
       to_date(opened_date) as opened_date, status
FROM {CAT}.retail_bronze.stores
""")
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.dim_suppliers AS
SELECT supplier_id, name as supplier_name, country, contact_email, rating, lead_time_days, status,
       CASE WHEN rating>=4.5 THEN 'Tier1' WHEN rating>=3.5 THEN 'Tier2' ELSE 'Tier3' END as tier
FROM {CAT}.retail_bronze.suppliers
""")
print(f"dim_stores: {spark.table(f'{CAT}.retail_silver.dim_stores').count()}, dim_suppliers: {spark.table(f'{CAT}.retail_silver.dim_suppliers').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_orders AS
WITH approved_refunds AS (
  SELECT order_id, SUM(CASE WHEN status='Approved' THEN refund_amount ELSE 0 END) AS approved_refund_amount
  FROM {CAT}.retail_bronze.returns
  GROUP BY order_id
)
SELECT o.order_id, o.customer_id, o.store_id,
       cast(date_format(to_date(o.order_date),'yyyyMMdd') as int) as order_date_key,
       to_date(o.order_date) as order_date, o.status as order_status, o.channel,
       cast(o.order_total as double) as order_total,
       cast(o.discount_amount as double) as discount_amount,
       cast(o.tax_amount as double) as tax_amount,
       round(o.order_total-o.discount_amount,2) AS net_sales_before_refunds,
       round(COALESCE(r.approved_refund_amount,0),2) AS approved_refund_amount,
       CASE WHEN o.status='Cancelled' THEN 0.0
            ELSE round(GREATEST(o.order_total-o.discount_amount-COALESCE(r.approved_refund_amount,0),0.0),2) END as net_revenue,
       o.currency, o._batch_id, o._run_id
FROM {CAT}.retail_bronze.orders o
LEFT JOIN approved_refunds r ON o.order_id=r.order_id
WHERE o.customer_id IS NOT NULL AND o.order_total>0
""")
print(f"fact_orders: {spark.table(f'{CAT}.retail_silver.fact_orders').count()}")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_order_items AS SELECT item_id, order_id, product_id, quantity, unit_price, line_total, discount_pct, round(line_total*(1-discount_pct),2) as net_line_total FROM {CAT}.retail_bronze.order_items WHERE quantity>0 AND unit_price>0")
print(f"fact_order_items: {spark.table(f'{CAT}.retail_silver.fact_order_items').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_payments AS
SELECT payment_id, order_id, amount, method, status as payment_status,
       to_date(payment_date) as payment_date, currency, transaction_ref
FROM {CAT}.retail_bronze.payments WHERE amount>0
""")
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_shipments AS
SELECT shipment_id, order_id, carrier, tracking_number, status as shipment_status,
       to_date(ship_date) as ship_date, to_date(estimated_delivery) as estimated_delivery,
       to_date(actual_delivery) as actual_delivery, weight_kg,
       CASE WHEN actual_delivery IS NULL THEN CAST(NULL AS BOOLEAN)
            WHEN to_date(actual_delivery)<=to_date(estimated_delivery) THEN true ELSE false END as delivered_on_time
FROM {CAT}.retail_bronze.shipments
""")
print(f"fact_payments: {spark.table(f'{CAT}.retail_silver.fact_payments').count()}, fact_shipments: {spark.table(f'{CAT}.retail_silver.fact_shipments').count()}")

# COMMAND ----------

# DBTITLE 1,Silver Facts: Returns & Inventory
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_returns AS
SELECT r.return_id, r.order_id, r.product_id, r.reason, to_date(r.return_date) as return_date,
       r.refund_amount, r.status as return_status,
       datediff(to_date(r.return_date),to_date(o.order_date)) as days_to_return
FROM {CAT}.retail_bronze.returns r
LEFT JOIN {CAT}.retail_bronze.orders o ON r.order_id=o.order_id
""")
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_silver.fact_inventory_daily_movement AS
SELECT product_id, warehouse_id, to_date(event_date) as snapshot_date,
       SUM(quantity) as net_quantity,
       SUM(CASE WHEN event_type IN ('receipt','return') THEN quantity ELSE 0 END) as total_received,
       SUM(CASE WHEN event_type='sale' THEN ABS(quantity) ELSE 0 END) as total_sold,
       COUNT(*) as event_count
FROM {CAT}.retail_bronze.inventory_events
GROUP BY product_id, warehouse_id, to_date(event_date)
""")
print(f"fact_returns: {spark.table(f'{CAT}.retail_silver.fact_returns').count()}, fact_inventory_daily_movement: {spark.table(f'{CAT}.retail_silver.fact_inventory_daily_movement').count()}")

# COMMAND ----------

# GOLD
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.daily_sales_summary AS
SELECT order_date, channel,
       COUNT(DISTINCT order_id) AS order_attempts,
       COUNT(DISTINCT CASE WHEN order_status <> 'Cancelled' THEN order_id END) AS total_orders,
       COUNT(DISTINCT customer_id) AS unique_customers,
       ROUND(SUM(net_revenue),2) AS total_revenue,
       ROUND(AVG(CASE WHEN order_status <> 'Cancelled' THEN net_revenue END),2) AS avg_order_value,
       ROUND(SUM(CASE WHEN order_status <> 'Cancelled' THEN discount_amount ELSE 0 END),2) AS total_discounts,
       COUNT(CASE WHEN order_status='Cancelled' THEN 1 END) AS cancelled_orders
FROM {CAT}.retail_silver.fact_orders GROUP BY order_date, channel
""")
print(f"daily_sales_summary: {spark.table(f'{CAT}.retail_gold.daily_sales_summary').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.customer_kpis AS
SELECT c.customer_id, c.full_name, c.customer_segment, c.country, c.status, c.registration_date, c.loyalty_points,
       COUNT(DISTINCT CASE WHEN o.order_status <> 'Cancelled' THEN o.order_id END) as total_orders,
       COALESCE(SUM(o.net_revenue),0) as total_revenue,
       COALESCE(AVG(CASE WHEN o.order_status <> 'Cancelled' THEN o.net_revenue END),0) as avg_order_value,
       COALESCE(MAX(CASE WHEN o.order_status <> 'Cancelled' THEN o.order_date END),c.registration_date) as last_order_date,
       datediff(current_date(), to_date(COALESCE(MAX(CASE WHEN o.order_status <> 'Cancelled' THEN o.order_date END),c.registration_date))) as days_since_last_order,
       datediff(current_date(), to_date(c.registration_date)) as tenure_days
FROM {CAT}.retail_silver.dim_customers c LEFT JOIN {CAT}.retail_silver.fact_orders o ON c.customer_id=o.customer_id
GROUP BY c.customer_id,c.full_name,c.customer_segment,c.country,c.status,c.registration_date,c.loyalty_points
""")
print(f"customer_kpis: {spark.table(f'{CAT}.retail_gold.customer_kpis').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.customer_lifetime_value AS
SELECT customer_id, full_name, customer_segment, total_revenue as historical_revenue, total_orders, avg_order_value, tenure_days,
       CASE WHEN tenure_days>=0 THEN round(total_revenue/GREATEST(tenure_days,30)*365*3,2) ELSE 0 END as baseline_3yr_clv,
       'historical_run_rate_floor_30d' as clv_method,
       CASE WHEN total_revenue>=5000 THEN 'Platinum' WHEN total_revenue>=2000 THEN 'Gold' WHEN total_revenue>=500 THEN 'Silver' ELSE 'Bronze' END as clv_tier
FROM {CAT}.retail_gold.customer_kpis
""")
print(f"customer_lifetime_value: {spark.table(f'{CAT}.retail_gold.customer_lifetime_value').count()}")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.customer_360 AS SELECT ck.customer_id, ck.full_name, ck.customer_segment, ck.country, ck.status, ck.total_orders, ck.total_revenue, ck.avg_order_value, ck.days_since_last_order, ck.tenure_days, ck.loyalty_points, clv.baseline_3yr_clv, clv.clv_tier, CASE WHEN ck.days_since_last_order>365 THEN 'High' WHEN ck.days_since_last_order>180 THEN 'Medium' ELSE 'Low' END as recency_risk FROM {CAT}.retail_gold.customer_kpis ck LEFT JOIN {CAT}.retail_gold.customer_lifetime_value clv ON ck.customer_id=clv.customer_id")
print(f"customer_360: {spark.table(f'{CAT}.retail_gold.customer_360').count()}")

# COMMAND ----------

# Aggregate sales and reviews separately; direct joins multiply sales by review count.
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.product_kpis AS
WITH sales AS (
  SELECT i.product_id, COUNT(DISTINCT i.order_id) as order_count,
         SUM(i.quantity) as total_units_sold, SUM(i.net_line_total) as sales_before_refunds
  FROM {CAT}.retail_silver.fact_order_items i
  JOIN {CAT}.retail_silver.fact_orders o ON i.order_id=o.order_id
  WHERE o.order_status <> 'Cancelled'
  GROUP BY i.product_id
), refunds AS (
  SELECT product_id, SUM(CASE WHEN return_status='Approved' THEN refund_amount ELSE 0 END) AS approved_refunds
  FROM {CAT}.retail_silver.fact_returns
  GROUP BY product_id
), reviews AS (
  SELECT product_id, AVG(rating) as avg_review_rating, COUNT(*) as review_count
  FROM {CAT}.retail_bronze.product_reviews GROUP BY product_id
)
SELECT p.product_id, p.product_name, p.category_name, p.price, p.margin_pct, p.status,
       COALESCE(s.order_count,0) as order_count,
       COALESCE(s.total_units_sold,0) as total_units_sold,
       ROUND(GREATEST(COALESCE(s.sales_before_refunds,0)-COALESCE(rf.approved_refunds,0),0.0),2) as total_revenue,
       ROUND(COALESCE(rf.approved_refunds,0),2) as approved_refunds,
       r.avg_review_rating, COALESCE(r.review_count,0) as review_count
FROM {CAT}.retail_silver.dim_products p
LEFT JOIN sales s ON p.product_id=s.product_id
LEFT JOIN refunds rf ON p.product_id=rf.product_id
LEFT JOIN reviews r ON p.product_id=r.product_id
""")
print(f"product_kpis: {spark.table(f'{CAT}.retail_gold.product_kpis').count()}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.store_performance AS
SELECT s.store_id, s.store_name, s.city, s.state, s.store_type,
       COUNT(DISTINCT o.order_id) AS order_attempts,
       COUNT(DISTINCT CASE WHEN o.order_status <> 'Cancelled' THEN o.order_id END) AS total_orders,
       COUNT(DISTINCT CASE WHEN o.order_status <> 'Cancelled' THEN o.customer_id END) AS unique_customers,
       COALESCE(SUM(o.net_revenue),0) AS total_revenue,
       COALESCE(AVG(CASE WHEN o.order_status <> 'Cancelled' THEN o.net_revenue END),0) AS avg_order_value
FROM {CAT}.retail_silver.dim_stores s
LEFT JOIN {CAT}.retail_silver.fact_orders o ON s.store_id=o.store_id
GROUP BY s.store_id,s.store_name,s.city,s.state,s.store_type
""")
spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.supplier_performance AS SELECT s.supplier_id, s.supplier_name, s.country, s.tier, s.rating, s.lead_time_days, COUNT(DISTINCT p.product_id) as product_count FROM {CAT}.retail_silver.dim_suppliers s LEFT JOIN {CAT}.retail_bronze.products p ON s.supplier_id=p.supplier_id GROUP BY s.supplier_id,s.supplier_name,s.country,s.tier,s.rating,s.lead_time_days")
print(f"store_performance: {spark.table(f'{CAT}.retail_gold.store_performance').count()}, supplier_performance: {spark.table(f'{CAT}.retail_gold.supplier_performance').count()}")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.inventory_health AS SELECT i.product_id, p.product_name, p.category_name, i.warehouse_id, SUM(i.net_quantity) as current_stock, SUM(i.total_sold) as total_sold, CASE WHEN SUM(i.net_quantity)<=0 THEN 'Out of Stock' WHEN SUM(i.net_quantity)<=10 THEN 'Critical' WHEN SUM(i.net_quantity)<=50 THEN 'Low' WHEN SUM(i.net_quantity)<=200 THEN 'Normal' ELSE 'Overstocked' END as stock_status, CASE WHEN SUM(i.net_quantity)<=10 THEN true ELSE false END as reorder_flag FROM {CAT}.retail_silver.fact_inventory_daily_movement i LEFT JOIN {CAT}.retail_silver.dim_products p ON i.product_id=p.product_id GROUP BY i.product_id,p.product_name,p.category_name,i.warehouse_id")
print(f"inventory_health: {spark.table(f'{CAT}.retail_gold.inventory_health').count()}")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.support_operations AS SELECT category, priority, status as ticket_status, COUNT(*) as ticket_count, round(AVG(satisfaction_score),2) as avg_satisfaction, COUNT(CASE WHEN resolved_date IS NOT NULL THEN 1 END) as resolved_count, round(COUNT(CASE WHEN resolved_date IS NOT NULL THEN 1 END)*100.0/COUNT(*),2) as resolution_rate_pct FROM {CAT}.retail_bronze.support_tickets GROUP BY category,priority,status")
print(f"support_operations: {spark.table(f'{CAT}.retail_gold.support_operations').count()}")

# COMMAND ----------

# DBTITLE 1,Executive KPI
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_gold.executive_kpi AS
WITH cust_stats AS (
  SELECT COUNT(*) AS total_customers,
         COUNT(CASE WHEN status='Active' THEN 1 END) AS active_customers
  FROM {CAT}.retail_silver.dim_customers
),
order_stats AS (
  SELECT COUNT(*) AS order_attempts,
         COUNT(CASE WHEN order_status <> 'Cancelled' THEN 1 END) AS total_orders,
         ROUND(SUM(net_revenue),2) AS total_revenue,
         ROUND(AVG(CASE WHEN order_status <> 'Cancelled' THEN net_revenue END),2) AS avg_order_value
  FROM {CAT}.retail_silver.fact_orders
),
return_stats AS (
  SELECT COUNT(*) AS total_returns FROM {CAT}.retail_silver.fact_returns
),
prod_stats AS (
  SELECT COUNT(CASE WHEN status='Active' THEN 1 END) AS active_products
  FROM {CAT}.retail_silver.dim_products
),
ticket_stats AS (
  SELECT COUNT(CASE WHEN status='open' THEN 1 END) AS open_tickets
  FROM {CAT}.retail_bronze.support_tickets
)
SELECT current_date() AS report_date,
       c.total_customers, c.active_customers,
       o.order_attempts, o.total_orders, o.total_revenue, o.avg_order_value,
       r.total_returns, p.active_products, t.open_tickets
FROM cust_stats c
CROSS JOIN order_stats o
CROSS JOIN return_stats r
CROSS JOIN prod_stats p
CROSS JOIN ticket_stats t
""")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_gold.executive_kpi IS 'Single-row executive KPI snapshot with cross-table aggregates computed via CTE scans'")
print("executive_kpi: 1")

# COMMAND ----------

# DBTITLE 1,Pipeline Run Log
curated_rows = sum([
    spark.table(f"{CAT}.retail_silver.fact_orders").count(),
    spark.table(f"{CAT}.retail_silver.fact_order_items").count(),
    spark.table(f"{CAT}.retail_gold.customer_360").count(),
    spark.table(f"{CAT}.retail_gold.product_kpis").count(),
])

# Metric Definitions (documented explicitly for business clarity):
# - net_revenue = order_total - discount_amount - approved_refund_amount (Cancelled orders = 0)
# - avg_order_value (AOV) = AVG(net_revenue) for non-cancelled orders only
# - baseline_3yr_clv = historical revenue run-rate projection (total_revenue / tenure_days * 365 * 3, floor 30 days)
# - recency_risk = rule-based High/Medium/Low based on days_since_last_order (>365/>180/<=180)
# - refund_amount = applies only when return status = 'Approved'

# Add governance comments for key curated tables
for tbl, comment in [
    ("retail_silver.dim_date", "Calendar dimension: date_key, year, quarter, month, week, day, season"),
    ("retail_silver.dim_customers", "Customer dimension with SCD columns (valid_from, valid_to, is_current)"),
    ("retail_silver.dim_products", "Product dimension with category join, gross margin, and margin_pct"),
    ("retail_silver.fact_orders", "Order fact: refund-aware net revenue with cancelled orders zeroed"),
    ("retail_silver.fact_order_items", "Order line items filtered to positive quantity and price"),
    ("retail_silver.fact_returns", "Return facts with days_to_return computed from order date"),
    ("retail_silver.fact_inventory_daily_movement", "Inventory daily movement: net_quantity, total_received, total_sold per product/warehouse/day"),
    ("retail_gold.daily_sales_summary", "Daily sales KPIs by order_date and channel with refund-aware revenue"),
    ("retail_gold.customer_kpis", "Customer KPIs: order count, revenue, recency, tenure"),
    ("retail_gold.customer_lifetime_value", "CLV: baseline_3yr_clv = historical revenue run-rate projection (not predictive ML), with tier classification (Bronze/Silver/Gold/Platinum)"),
    ("retail_gold.customer_360", "Unified customer view joining KPIs, CLV, and recency_risk (rule-based recency categorization, not ML churn prediction)"),
    ("retail_gold.product_kpis", "Product KPIs: sales, refunds, and reviews aggregated independently to avoid fan-out"),
    ("retail_gold.store_performance", "Store performance: order attempts, revenue, and AOV"),
    ("retail_gold.supplier_performance", "Supplier performance: product count and rating"),
    ("retail_gold.inventory_health", "Inventory health: stock status, reorder flags, and current stock levels"),
    ("retail_gold.support_operations", "Support operations: ticket counts, satisfaction, and resolution rates by category/priority/status"),
]:
    try:
        spark.sql(f"COMMENT ON TABLE {CAT}.{tbl} IS '{comment}'")
    except Exception:
        pass

spark.sql(f"""
INSERT INTO {CAT}.retail_monitoring.pipeline_runs
VALUES ('{RUN_ID}','02_silver_gold_pipeline','silver_gold','SUCCEEDED',{curated_rows},timestamp('{_silver_start}'),current_timestamp(),{round((datetime.now(timezone.utc).replace(tzinfo=None) - _silver_start).total_seconds(), 2)},'{PROJECT_VERSION}','free-edition','')
""")
print(f"Silver & Gold complete. Key curated rows: {curated_rows:,}")
