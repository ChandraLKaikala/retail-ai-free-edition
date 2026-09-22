# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Ingestion — Free Edition E2E v4.0
# MAGIC Creates **326,308 deterministic synthetic rows across 15 Bronze tables**, anchored to the Spark session's current date.
# MAGIC
# MAGIC The generator enforces stronger realism: age-aware order states, coherent payment state, cancelled orders are never shipped, every `Returned` order has an approved purchased-product return, inventory begins with opening stock and cannot become physically negative, refunds are capped to purchased net value, and verified reviews are tied to completed/returned purchases.

# COMMAND ----------

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
import uuid, random
from datetime import datetime, timedelta, date, timezone
from pyspark.sql import Row
from pyspark.sql.functions import lit

RUN_ID = str(uuid.uuid4())[:8]
NOW = datetime.now(timezone.utc).replace(tzinfo=None)
BATCH_ID = f"BATCH_{NOW:%Y%m%d_%H%M%S}"
SEED = 42
BASE = spark.sql("SELECT current_date() AS d").first()["d"]

def rng(i, offset=0):
    return random.Random(SEED + i + offset)

def save(df, table, mode="overwrite"):
    (df.withColumn("_batch_id", lit(BATCH_ID))
       .withColumn("_run_id", lit(RUN_ID))
       .withColumn("_ingestion_ts", lit(NOW).cast("timestamp"))
       .write.format("delta").mode(mode).option("overwriteSchema","true")
       .saveAsTable(f"{CAT}.retail_bronze.{table}"))

print(f"Run: {RUN_ID} | Batch: {BATCH_ID} | Anchor date: {BASE}")

# COMMAND ----------

save(spark.createDataFrame([
  ("CAT001","Electronics","Phones and computers"),("CAT002","Clothing","Apparel"),
  ("CAT003","Home & Kitchen","Household"),("CAT004","Sports","Sports and outdoors"),
  ("CAT005","Books","Books and media"),("CAT006","Beauty","Health and beauty"),
  ("CAT007","Toys","Toys and games"),("CAT008","Food","Grocery"),
], ["category_id","name","description"]), "categories")
print("categories: 8")

# COMMAND ----------

SNAMES = ["Apex","Nexus","Prime","Global","Delta","Pacific","Atlantic","Summit","Pinnacle","Elite"]
SCOUNTRIES = ["US","CN","DE","IN","JP","GB","FR","CA","AU","BR"]
sup = [Row(supplier_id=f"SUP{i:04d}", name=f"{rng(i,100).choice(SNAMES)} Corp {i}",
           country=rng(i,100).choice(SCOUNTRIES), contact_email=f"sup{i}@supply.com",
           rating=round(rng(i,100).uniform(2.5,5.0),1), lead_time_days=rng(i,100).randint(3,30),
           status=rng(i,100).choice(["Active","Active","Active","Inactive"])) for i in range(1,151)]
save(spark.createDataFrame(sup), "suppliers")
print(f"suppliers: {len(sup)}")

# COMMAND ----------

STORE_LOCS = [
    ("New York","NY"),("Los Angeles","CA"),("Chicago","IL"),("Houston","TX"),
    ("Phoenix","AZ"),("Philadelphia","PA"),("San Antonio","TX"),("San Diego","CA")
]
stores = []
for i in range(1,51):
    r = rng(i,500)
    city, state = r.choice(STORE_LOCS)
    stores.append(Row(store_id=f"STR{i:04d}", name=f"RetailHub Store {i}", city=city, state=state,
                      country="US", store_type=r.choice(["Flagship","Standard","Express","Outlet"]),
                      size_sqft=r.randint(2000,50000),
                      opened_date=str(date(2010+r.randint(0,13), r.randint(1,12), r.randint(1,28))),
                      status="Active"))
save(spark.createDataFrame(stores), "stores")
print(f"stores: {len(stores)}")

# COMMAND ----------

PNAMES = ["Pro","Plus","Max","Ultra","Mini","Lite","Elite","Smart","Classic","Premium"]
PTYPES = ["Widget","Device","Kit","Pack","Set","Bundle","System","Unit","Module","Hub"]
prods = []
for i in range(1,1001):
    r = rng(i,1000)
    bp = round(r.uniform(5.0,999.99),2)
    prods.append(Row(product_id=f"PRD{i:05d}", name=f"{r.choice(PNAMES)} {r.choice(PTYPES)} {i}",
                     category_id=f"CAT{r.randint(1,8):03d}", supplier_id=f"SUP{r.randint(1,150):04d}",
                     price=bp, cost=round(bp*r.uniform(0.3,0.7),2), sku=f"SKU{i:07d}",
                     weight_kg=round(r.uniform(0.1,25.0),2),
                     status=r.choice(["Active","Active","Active","Discontinued"]),
                     created_date=str(date(2019+r.randint(0,5), r.randint(1,12), r.randint(1,28)))))
save(spark.createDataFrame(prods), "products")
print(f"products: {len(prods)}")

# COMMAND ----------

FNAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah","Thomas","Karen","Charles","Lisa"]
LNAMES = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Moore"]
SEGS   = ["Premium","Standard","Basic","VIP","Standard","Standard"]
CUSTOMER_LOCS = [
    ("US","New York"),("US","Los Angeles"),("US","Chicago"),("US","Houston"),("US","Phoenix"),
    ("CA","Toronto"),("GB","London"),("AU","Sydney"),("DE","Berlin"),("FR","Paris"),("MX","Mexico City")
]
custs  = []
for i in range(1,10001):
    r = rng(i,2000)
    country, city = r.choice(CUSTOMER_LOCS)
    custs.append(Row(customer_id=f"C{i:07d}", first_name=r.choice(FNAMES), last_name=r.choice(LNAMES),
                     email=f"user{i}@{'gmail' if i%3==0 else 'yahoo' if i%3==1 else 'outlook'}.com",
                     phone=f"555-{r.randint(1000000,9999999)}",
                     date_of_birth=str(date(1960+r.randint(0,45), r.randint(1,12), r.randint(1,28))),
                     customer_segment=r.choice(SEGS),
                     registration_date=str(BASE - timedelta(days=r.randint(0,2000))),
                     country=country, city=city,
                     status=r.choice(["Active","Active","Active","Inactive","Churned"]),
                     loyalty_points=r.randint(0,50000)))
save(spark.createDataFrame(custs), "customers")
print(f"customers: {len(custs)}")

# COMMAND ----------

CHANS   = ["web","mobile","store","mobile","web"]
CIDS    = [f"C{i:07d}" for i in range(1,10001)]
SIDS    = [f"STR{i:04d}" for i in range(1,51)]
orders  = []
for i in range(1,30001):
    r = rng(i,5000)
    order_dt = BASE - timedelta(days=r.randint(0,730))
    age = (BASE - order_dt).days
    if age == 0:
        status = r.choice(["Processing","Processing","Cancelled"])
    elif age <= 2:
        status = r.choice(["Processing","Shipped","Shipped","Cancelled"])
    elif age <= 7:
        status = r.choice(["Shipped","Completed","Completed","Cancelled"])
    else:
        status = r.choice(["Completed","Completed","Completed","Completed","Returned","Cancelled"])
    orders.append(Row(order_id=f"ORD{i:08d}", customer_id=r.choice(CIDS), store_id=r.choice(SIDS),
                      order_date=str(order_dt), status=status, channel=r.choice(CHANS),
                      order_total=0.0, discount_amount=0.0, tax_amount=0.0, currency="USD"))
save(spark.createDataFrame(orders), "orders")
ORDER_DATES = {o.order_id: date.fromisoformat(o.order_date) for o in orders}
ORDER_CUSTOMERS = {o.order_id: o.customer_id for o in orders}
ORDER_STATUS = {o.order_id: o.status for o in orders}
print(f"orders: {len(orders)}")

# COMMAND ----------

from collections import defaultdict

PIDS = [f"PRD{i:05d}" for i in range(1,1001)]
OIDS = [f"ORD{i:08d}" for i in range(1,30001)]
PRICE_BY_PID = {p.product_id: float(p.price) for p in prods}
ORDER_PRODUCTS = defaultdict(list)
ORDER_PRODUCT_NET = defaultdict(float)
items = []
for i in range(1,75001):
    r = rng(i,10000)
    oid = OIDS[i-1] if i <= len(OIDS) else r.choice(OIDS)
    pid = r.choice(PIDS)
    qty = r.randint(1,10)
    price = PRICE_BY_PID[pid]
    discount_pct = round(r.uniform(0,0.3),2)
    line_total = round(qty*price,2)
    items.append(Row(item_id=f"ITM{i:09d}", order_id=oid, product_id=pid,
                     quantity=qty, unit_price=price, line_total=line_total,
                     discount_pct=discount_pct))
    ORDER_PRODUCTS[oid].append(pid)
    ORDER_PRODUCT_NET[(oid,pid)] += round(line_total*(1-discount_pct),2)
save(spark.createDataFrame(items), "order_items")

spark.sql(f"""
MERGE INTO {CAT}.retail_bronze.orders AS o
USING (
  SELECT order_id, ROUND(SUM(line_total),2) AS item_total,
         ROUND(SUM(line_total * discount_pct),2) AS discount_total
  FROM {CAT}.retail_bronze.order_items GROUP BY order_id
) AS i ON o.order_id = i.order_id
WHEN MATCHED THEN UPDATE SET
  o.order_total = i.item_total,
  o.discount_amount = i.discount_total,
  o.tax_amount = ROUND((i.item_total - i.discount_total) * 0.08, 2)
""")
ORDER_FINANCIALS = {r['order_id']: (float(r['order_total']), float(r['discount_amount']), float(r['tax_amount']))
                    for r in spark.sql(f"SELECT order_id, order_total, discount_amount, tax_amount FROM {CAT}.retail_bronze.orders").collect()}
ORDER_TOTALS = {oid: vals[0] for oid, vals in ORDER_FINANCIALS.items()}
ORDER_PAYABLES = {oid: round(vals[0]-vals[1]+vals[2],2) for oid, vals in ORDER_FINANCIALS.items()}
print(f"order_items: {len(items)} | order headers reconciled: {len(ORDER_TOTALS)}")

# COMMAND ----------

PMETHODS = ["credit_card","debit_card","paypal","apple_pay","bank_transfer"]
pays = []
for i in range(1,32001):
    r = rng(i,20000)
    if i <= len(OIDS):
        oid = OIDS[i-1]
        # Treat status as the current state of the primary payment: all live orders are paid;
        # cancelled orders are represented as refunded rather than as recognized revenue.
        status = "Refunded" if ORDER_STATUS[oid] == "Cancelled" else "Completed"
        amount = ORDER_PAYABLES.get(oid, 0.0)
    else:
        # Extra rows represent failed payment attempts and do not create fake recognized revenue.
        oid = r.choice(OIDS)
        status = "Failed"
        amount = ORDER_PAYABLES.get(oid, 0.0)
    pay_date = min(BASE, ORDER_DATES[oid] + timedelta(days=r.randint(0,2)))
    pays.append(Row(payment_id=f"PAY{i:08d}", order_id=oid,
                    amount=max(round(amount,2),0.01), method=r.choice(PMETHODS), status=status,
                    payment_date=str(pay_date), currency="USD", transaction_ref=f"TXN{i:010d}"))
save(spark.createDataFrame(pays), "payments")
print(f"payments: {len(pays)} | primary payment state reconciled to order status")

# COMMAND ----------

CARRIERS = ["FedEx","UPS","USPS","DHL","Amazon Logistics"]
SHIP_ELIGIBLE_OIDS = [oid for oid in OIDS if ORDER_STATUS[oid] != "Cancelled"]
ships = []
for i in range(1,28001):
    r = rng(i,30000)
    oid = SHIP_ELIGIBLE_OIDS[(i-1) % len(SHIP_ELIGIBLE_OIDS)]
    order_status = ORDER_STATUS[oid]
    sd = min(BASE, ORDER_DATES[oid] + timedelta(days=r.randint(0,2)))
    eta = sd + timedelta(days=r.randint(1,7))
    if order_status == "Processing":
        status, actual = "Processing", None
    elif order_status == "Shipped":
        status = "Delivered" if eta <= BASE and r.random() < 0.35 else "In Transit"
        actual = min(BASE, max(sd, eta + timedelta(days=r.randint(-1,2)))) if status == "Delivered" else None
    else:
        status = "Delivered"
        actual = min(BASE, max(sd, eta + timedelta(days=r.randint(-1,2))))
    ships.append(Row(shipment_id=f"SHP{i:08d}", order_id=oid,
                     carrier=r.choice(CARRIERS), tracking_number=f"TRK{i:012d}",
                     status=status, ship_date=str(sd), estimated_delivery=str(eta),
                     actual_delivery=str(actual) if actual else None,
                     weight_kg=round(r.uniform(0.1,50.0),2)))
save(spark.createDataFrame(ships), "shipments")
print(f"shipments: {len(ships)} | cancelled orders shipped: 0 by construction")

# COMMAND ----------

WAREHOUSES = ["WH001","WH002","WH003","WH004","WH005"]
ITYPES     = ["receipt","sale","adjustment","return","transfer"]
invs = []
opening_pairs = [(pid, wh) for pid in PIDS for wh in WAREHOUSES]
running_stock = {}
for i in range(1,40001):
    r = rng(i,40000)
    if i <= len(opening_pairs):
        pid, wh = opening_pairs[i-1]
        event_type, qty, event_dt = "receipt", r.randint(150,500), BASE - timedelta(days=730)
    else:
        pid, wh = r.choice(PIDS), r.choice(WAREHOUSES)
        current = running_stock.get((pid,wh),0)
        event_type = r.choice(ITYPES)
        if event_type in ("receipt","return"):
            qty = r.randint(1,200)
        elif event_type == "sale":
            if current <= 0:
                event_type, qty = "receipt", r.randint(25,150)
            else:
                qty = -r.randint(1,min(50,current))
        else:  # adjustment / transfer leg
            if current <= 0 or r.random() >= 0.5:
                qty = r.randint(1,80)
            else:
                qty = -r.randint(1,min(80,current))
        event_dt = BASE - timedelta(days=r.randint(0,729))
    running_stock[(pid,wh)] = running_stock.get((pid,wh),0) + qty
    if running_stock[(pid,wh)] < 0:
        raise AssertionError(f"Negative generated stock for {(pid,wh)}")
    invs.append(Row(event_id=f"INV{i:08d}", product_id=pid, warehouse_id=wh,
                    event_type=event_type, quantity=qty, event_date=str(event_dt),
                    unit_cost=round(r.uniform(1.0,500.0),2)))
save(spark.createDataFrame(invs), "inventory_events")
print(f"inventory_events: {len(invs)} | opening stock pairs: {len(opening_pairs)} | negative final stock: 0")

# COMMAND ----------

REASONS = ["Defective","Wrong item","Changed mind","Size issue","Quality issue","Damaged shipping"]
from collections import defaultdict
eligible_by_order = defaultdict(list)
for (oid,pid), net_value in ORDER_PRODUCT_NET.items():
    if ORDER_DATES[oid] < BASE and ORDER_STATUS[oid] in ("Completed","Returned"):
        eligible_by_order[oid].append(pid)

returned_oids = [oid for oid in OIDS if ORDER_DATES[oid] < BASE and ORDER_STATUS[oid] == "Returned"]
if len(returned_oids) > 5000:
    raise RuntimeError(f"Returned-order count exceeds return-row budget: {len(returned_oids)}")

selected = []
used_pairs = set()
for idx, oid in enumerate(returned_oids, 1):
    candidates = sorted(set(eligible_by_order.get(oid, [])))
    if not candidates:
        raise RuntimeError(f"Returned order has no purchased product: {oid}")
    pid = rng(idx,51000).choice(candidates)
    selected.append((oid,pid,True))
    used_pairs.add((oid,pid))

remaining_pairs = [(oid,pid) for oid,pids in eligible_by_order.items() for pid in sorted(set(pids))
                   if (oid,pid) not in used_pairs]
return_rng = random.Random(SEED + 50000)
return_rng.shuffle(remaining_pairs)
needed = 5000 - len(selected)
if len(remaining_pairs) < needed:
    raise RuntimeError(f"Not enough eligible order/product pairs for returns: need {needed}, have {len(remaining_pairs)}")
selected.extend((oid,pid,False) for oid,pid in remaining_pairs[:needed])

rets = []
for i, (oid,pid,forced_returned) in enumerate(selected, 1):
    r = rng(i,50000)
    age_days = (BASE - ORDER_DATES[oid]).days
    return_dt = ORDER_DATES[oid] + timedelta(days=r.randint(1, min(60, age_days)))
    purchased_net = max(0.01, round(ORDER_PRODUCT_NET[(oid,pid)],2))
    refund_amount = min(round(purchased_net * r.uniform(0.25,1.0),2), purchased_net)
    return_status = "Approved" if forced_returned else r.choice(["Approved","Approved","Pending","Rejected"])
    rets.append(Row(return_id=f"RET{i:07d}", order_id=oid, product_id=pid,
                    reason=r.choice(REASONS), return_date=str(return_dt), refund_amount=refund_amount,
                    status=return_status))
save(spark.createDataFrame(rets), "returns")
print(f"returns: {len(rets)} | Returned orders covered by an approved return: {len(returned_oids)}")

# COMMAND ----------

PAGES    = ["home","product","cart","checkout","search","account","wishlist","category"]
BROWSERS = ["Chrome","Safari","Firefox","Edge","Mobile Safari"]
ETYPES   = ["view","click","add_to_cart","purchase","search"]
for batch in range(4):
    s = batch*25000+1; e = s+25000
    clicks = [Row(event_id=f"CLK{i:09d}", session_id=f"SES{rng(i,60000).randint(1,20000):08d}",
                  customer_id=f"C{rng(i,60000).randint(1,10000):07d}",
                  page=rng(i,60000).choice(PAGES), event_type=rng(i,60000).choice(ETYPES),
                  browser=rng(i,60000).choice(BROWSERS),
                  event_timestamp=str(NOW-timedelta(days=rng(i,60000).randint(0,90),seconds=rng(i,60000).randint(0,86399))),
                  product_id=rng(i,60000).choice(PIDS) if rng(i,60000).random()>0.5 else None,
                  duration_seconds=rng(i,60000).randint(1,600)) for i in range(s,e)]
    mode = "overwrite" if batch==0 else "append"
    (spark.createDataFrame(clicks)
     .withColumn("_batch_id", lit(BATCH_ID)).withColumn("_run_id", lit(RUN_ID))
     .withColumn("_ingestion_ts", lit(NOW).cast("timestamp"))
     .write.format("delta").mode(mode).option("overwriteSchema","true")
     .saveAsTable(f"{CAT}.retail_bronze.clickstream_events"))
    print(f"clickstream batch {batch+1}/4 done")

# COMMAND ----------

TCATS  = ["billing","shipping","product","account","returns","technical"]
TPRIS  = ["low","medium","high","critical"]
TSTAT  = ["resolved","resolved","resolved","open","pending","escalated"]
tkts = []
for i in range(1,3001):
    r = rng(i,70000)
    created = BASE - timedelta(days=r.randint(0,365))
    status = r.choice(TSTAT)
    resolved = min(BASE, created + timedelta(days=r.randint(0,14))) if status == "resolved" else None
    tkts.append(Row(ticket_id=f"TKT{i:07d}", customer_id=f"C{r.randint(1,10000):07d}",
                    category=r.choice(TCATS), priority=r.choice(TPRIS), status=status,
                    subject=f"Issue with {r.choice(TCATS)} #{i}",
                    created_date=str(created),
                    resolved_date=str(resolved) if resolved else None,
                    satisfaction_score=r.randint(1,5) if r.random()>0.4 else None))
save(spark.createDataFrame(tkts), "support_tickets")
print(f"support_tickets: {len(tkts)}")

# COMMAND ----------

REVIEWABLE_OIDS = [oid for oid in OIDS if ORDER_DATES[oid] < BASE and ORDER_STATUS[oid] in ("Completed","Returned")]
revs = []
for i in range(1,2001):
    r = rng(i,80000); rating = r.randint(1,5)
    verified = r.random()>0.2
    if verified:
        oid = r.choice(REVIEWABLE_OIDS)
        review_pid = r.choice(ORDER_PRODUCTS[oid])
        review_cid = ORDER_CUSTOMERS[oid]
        earliest_review = ORDER_DATES[oid] + timedelta(days=1)
    else:
        review_pid = r.choice(PIDS); review_cid = f"C{r.randint(1,10000):07d}"
        earliest_review = BASE - timedelta(days=730)
    review_date = earliest_review + timedelta(days=r.randint(0, max(0, (BASE-earliest_review).days)))
    revs.append(Row(review_id=f"REV{i:07d}", product_id=review_pid, customer_id=review_cid,
                    rating=rating, title=f"{'Great' if rating>=4 else 'OK' if rating==3 else 'Poor'} product",
                    body=f"Review #{i}. {'Highly recommend!' if rating>=4 else 'Average quality.'}",
                    review_date=str(review_date), verified_purchase=verified, helpful_votes=r.randint(0,100)))
save(spark.createDataFrame(revs), "product_reviews")
print(f"product_reviews: {len(revs)}")

# COMMAND ----------

DTYPES = ["policy","manual","runbook","guide","faq"]
TOPICS = ["returns","shipping","billing","loyalty","privacy","security","product_care","account","promotions","warranty","international","b2b","sustainability","accessibility","api"]
docs = []
for i in range(1,101):
    r = rng(i,90000); topic = TOPICS[i%len(TOPICS)]
    effective = BASE - timedelta(days=r.randint(0,365))
    docs.append(Row(doc_id=f"DOC{i:05d}",
                    title=f"{topic.replace('_',' ').title()} {r.choice(DTYPES).title()} v{r.randint(1,5)}",
                    doc_type=r.choice(DTYPES), topic=topic,
                    content=(f"RetailHub {topic} guidance: customers receive service according to the current policy. "
                             f"Effective {effective.isoformat()}. Contact support@retailhub.com. Doc ID DOC{i:05d}."),
                    effective_date=str(effective), version=r.randint(1,5), status="Active"))
save(spark.createDataFrame(docs), "knowledge_documents")
print(f"knowledge_documents: {len(docs)}")

# COMMAND ----------

TOTAL_ROWS = 326_308
spark.sql(f"""
INSERT INTO {CAT}.retail_monitoring.pipeline_runs
VALUES ('{RUN_ID}','01_bronze_ingestion','bronze','SUCCEEDED',{TOTAL_ROWS},current_timestamp(),current_timestamp(),'')
""")
print(f"Bronze complete: 15 tables | {TOTAL_ROWS:,} baseline rows.")
