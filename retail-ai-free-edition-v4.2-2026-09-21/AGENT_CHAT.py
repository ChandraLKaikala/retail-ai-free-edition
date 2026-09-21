# Databricks notebook source
# MAGIC %md
# MAGIC # RetailHub AI Agent — Interactive Chat
# MAGIC **No app needed.** Type your question in the widget below and run the cell.
# MAGIC
# MAGIC **Example questions:**
# MAGIC - What is the return policy?
# MAGIC - Show me revenue for the last 7 days
# MAGIC - Which products are out of stock?
# MAGIC - Tell me about the loyalty program
# MAGIC - Show data quality results
# MAGIC - Which customers are at high churn risk?
# MAGIC - What is the pipeline status?
# MAGIC - Show anomaly details

# COMMAND ----------

from pyspark.sql import functions as F
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

# ── TOOLS ──────────────────────────────────────────────────────────────────
def search_knowledge(query, limit=5):
    terms = [t.lower() for t in re.findall(r"[A-Za-z0-9_]+", str(query)) if len(t) > 3][:8]
    df = spark.table(f"{CAT}.retail_genai.document_chunks")
    if terms:
        predicate = None
        for term in terms:
            p = F.lower(F.col("chunk_text")).contains(term) | F.lower(F.col("title")).contains(term)
            predicate = p if predicate is None else (predicate | p)
        df = df.where(predicate)
    rows = df.select("chunk_id","title","topic","chunk_text").limit(max(1,min(int(limit),20))).collect()
    if not rows:
        return "No matching documents found."
    out = []
    for r in rows:
        out.append(f"**{r.title}** (topic: {r.topic})\n> {r.chunk_text[:300]}...")
    return "\n\n".join(out)

def get_sales_summary(days=7):
    rows = spark.sql(f"""
        SELECT order_date, SUM(total_orders) as orders,
               ROUND(SUM(total_revenue),2) as revenue,
               ROUND(SUM(total_revenue)/NULLIF(SUM(total_orders),0),2) as aov
        FROM {CAT}.retail_gold.daily_sales_summary
        WHERE to_date(order_date) >= date_sub(current_date(),{days})
        GROUP BY order_date ORDER BY order_date DESC LIMIT 10""").collect()
    if not rows: return "No sales data found."
    lines = ["| Date | Orders | Revenue | AOV |", "|------|--------|---------|-----|"]
    for r in rows:
        lines.append(f"| {r.order_date} | {r.orders:,} | ${r.revenue:,.2f} | ${r.aov:,.2f} |")
    return "\n".join(lines)

def get_customer_360(customer_id):
    rows = (spark.table(f"{CAT}.retail_gold.customer_360")
            .where(F.col("customer_id") == str(customer_id))
            .select("customer_id","full_name","customer_segment","country","status","total_orders",
                    F.round("total_revenue",2).alias("revenue"),"days_since_last_order","churn_risk","clv_tier")
            .limit(1).collect())
    if not rows: return f"Customer {customer_id} not found."
    r = rows[0]
    return (f"**{r.full_name}** ({r.customer_id})\n"
            f"- Segment: {r.customer_segment} | Status: {r.status} | Country: {r.country}\n"
            f"- Orders: {r.total_orders} | Revenue: ${r.revenue:,.2f}\n"
            f"- Days since last order: {r.days_since_last_order}\n"
            f"- Churn Risk: **{r.churn_risk}** | CLV Tier: **{r.clv_tier}**")

def get_inventory_risk(limit=15):
    rows = spark.sql(f"""
        SELECT product_name, warehouse_id, current_stock, stock_status
        FROM {CAT}.retail_gold.inventory_health
        WHERE stock_status IN ('Out of Stock','Critical','Low')
        ORDER BY current_stock ASC LIMIT {limit}""").collect()
    if not rows: return "No inventory risk found."
    lines = ["| Product | Warehouse | Stock | Status |", "|---------|-----------|-------|--------|"]
    for r in rows:
        lines.append(f"| {r.product_name[:30]} | {r.warehouse_id} | {r.current_stock} | **{r.stock_status}** |")
    return "\n".join(lines)

def get_high_churn_customers(limit=10):
    rows = spark.sql(f"""
        SELECT c.customer_id, c.full_name, c.customer_segment,
               m.churn_probability, c.total_revenue, c.days_since_last_order
        FROM {CAT}.retail_ml.churn_predictions m
        JOIN {CAT}.retail_gold.customer_360 c ON m.customer_id = c.customer_id
        WHERE m.churn_probability >= 0.6
        ORDER BY m.churn_probability DESC LIMIT {limit}""").collect()
    if not rows: return "No high-risk customers found."
    lines = ["| Customer | Segment | Churn Prob | Revenue | Days Inactive |",
             "|----------|---------|------------|---------|---------------|"]
    for r in rows:
        lines.append(f"| {r.full_name} | {r.customer_segment} | {r.churn_probability:.2%} | ${r.total_revenue:,.0f} | {r.days_since_last_order} |")
    return "\n".join(lines)

def get_anomaly_details(limit=10):
    rows = spark.sql(f"""
        SELECT entity_id, ROUND(anomaly_score,4) as score,
               ROUND(churn_probability,4) as churn_prob
        FROM {CAT}.retail_monitoring.anomaly_events
        ORDER BY churn_probability DESC LIMIT {limit}""").collect()
    if not rows: return "No anomaly events found."
    lines = ["| Customer ID | Anomaly Score | Churn Prob |", "|-------------|---------------|------------|"]
    for r in rows:
        lines.append(f"| {r.entity_id} | {r.score} | {r.churn_prob:.2%} |")
    return "\n".join(lines)

def get_data_quality_results():
    rows = spark.sql(f"""
        SELECT severity, rule_count, passed, failed, errors, pass_rate_pct
        FROM {CAT}.retail_quality.quality_summary
        ORDER BY severity""").collect()
    if not rows: return "No quality results found."
    lines = ["| Severity | Rules | Passed | Failed | Errors | Pass Rate |",
             "|----------|-------|--------|--------|--------|-----------|"]
    for r in rows:
        lines.append(f"| {r.severity} | {r.rule_count} | {r.passed} | {r.failed} | {r.errors} | **{r.pass_rate_pct}%** |")
    return "\n".join(lines)

def get_pipeline_status():
    rows = spark.sql(f"""
        SELECT notebook, layer, status, rows_written
        FROM {CAT}.retail_monitoring.pipeline_runs
        ORDER BY end_time DESC LIMIT 10""").collect()
    if not rows: return "No pipeline runs found."
    lines = ["| Notebook | Layer | Status | Rows |", "|----------|-------|--------|------|"]
    for r in rows:
        icon = "OK" if r.status == "SUCCEEDED" else "FAIL"
        lines.append(f"| {r.notebook} | {r.layer} | [{icon}] {r.status} | {r.rows_written:,} |")
    return "\n".join(lines)

def get_executive_kpi():
    rows = spark.sql(f"SELECT * FROM {CAT}.retail_gold.executive_kpi LIMIT 1").collect()
    if not rows: return "No KPI data."
    r = rows[0]
    return (f"**Platform Executive Summary**\n\n"
            f"| Metric | Value |\n|--------|-------|\n"
            f"| Total Customers | {int(r.total_customers):,} |\n"
            f"| Active Customers | {int(r.active_customers):,} |\n"
            f"| Recognized Orders | {int(r.total_orders):,} |\n"
            f"| Order Attempts | {int(r.order_attempts):,} |\n"
            f"| Total Revenue | ${float(r.total_revenue):,.2f} |\n"
            f"| Avg Order Value | ${float(r.avg_order_value):,.2f} |\n"
            f"| Total Returns | {int(r.total_returns):,} |\n"
            f"| Active Products | {int(r.active_products):,} |\n"
            f"| Open Tickets | {int(r.open_tickets):,} |")

def get_ml_model_summary():
    churn = spark.sql(f"""SELECT COUNT(*) as total,
        SUM(CASE WHEN churn_probability>=0.7 THEN 1 ELSE 0 END) as high_risk,
        ROUND(AVG(churn_probability)*100,1) as avg_prob
        FROM {CAT}.retail_ml.churn_predictions""").collect()[0]
    segs = spark.sql(f"""SELECT segment_label, COUNT(*) as n
        FROM {CAT}.retail_ml.customer_segments GROUP BY segment_label ORDER BY n DESC""").collect()
    demand_top = spark.sql(f"""SELECT product_id, predicted_units
        FROM {CAT}.retail_ml.demand_forecast ORDER BY predicted_units DESC LIMIT 5""").collect()
    out = (f"**Churn Model** (Random Forest)\n"
           f"- Scored: {int(churn.total):,} customers\n"
           f"- High Risk (>=70%): {int(churn.high_risk):,}\n"
           f"- Avg Churn Probability: {churn.avg_prob}%\n\n"
           f"**Segmentation** (KMeans k=4)\n")
    for s in segs:
        out += f"- {s.segment_label}: {s.n:,} customers\n"
    out += f"\n**Demand Baseline** (Top 5 products by estimated units)\n"
    for d in demand_top:
        out += f"- {d.product_id}: {d.predicted_units:,} units\n"
    return out


def get_realtime_status(limit=10):
    sources = [
        (f"{CAT}.retail_gold.live_sales_minute", "AvailableNow"),
        (f"{CAT}.retail_realtime.live_sales_minute_pipeline", "Lakeflow"),
    ]
    for table_name, mode in sources:
        try:
            if not spark.catalog.tableExists(table_name):
                continue
            rows = spark.sql(f"""
                SELECT event_minute, channel, order_count, ROUND(revenue,2) AS revenue, last_updated
                FROM {table_name}
                ORDER BY event_minute DESC, revenue DESC LIMIT {int(limit)}
            """).collect()
            if rows:
                lines = [f"**Live Sales — {mode}**", "| Minute | Channel | Orders | Revenue |", "|--------|---------|--------|---------|"]
                for r in rows:
                    lines.append(f"| {r.event_minute} | {r.channel} | {r.order_count:,} | ${r.revenue:,.2f} |")
                return "\n".join(lines)
        except Exception:
            pass
    return "No realtime output yet. Run 06_realtime_streaming, or deploy/run the DAB Lakeflow pipeline."

# ── SAFETY FILTER ──────────────────────────────────────────────────────────
INJECTION_PATTERNS = [
    "ignore previous","ignore instructions","system prompt","reveal credentials",
    "drop table","delete from","insert into","you are now","pretend you are",
    "act as","jailbreak","bypass","override safety","admin access","give me access",
    "show me secrets","api key","password","what is your prompt"
]

def is_safe(query):
    q = query.lower()
    for p in INJECTION_PATTERNS:
        if p in q:
            return False, p
    return True, None

# ── ROUTING ────────────────────────────────────────────────────────────────
ROUTES = [
    (["realtime","real-time","live","stream"], "realtime"),
    (["return","refund","policy","shipping","loyalty","warranty","privacy","billing","security","sustainability","international","b2b"], "knowledge"),
    (["sales","revenue","daily","trend","channel","orders last"], "sales"),
    (["executive","kpi","summary","platform","overview","dashboard"], "kpi"),
    (["inventory","stock","reorder","out of stock","warehouse","critical"], "inventory"),
    (["churn","at risk","high risk","losing","retention"], "churn"),
    (["anomaly","anomalies","fraud","suspicious","outlier","unusual"], "anomaly"),
    (["quality","dq","rules","pass rate","validation"], "quality"),
    (["pipeline","job","status","monitoring","run","notebook"], "pipeline"),
    (["model","ml","machine learning","prediction","forecast","segment"], "ml"),
    (["customer","profile","c0","who is"], "customer"),
]

def route(query):
    q = query.lower()
    def matches(keyword):
        return keyword in q if (" " in keyword or "-" in keyword) else re.search(rf"\b{re.escape(keyword)}\b", q) is not None
    for keywords, tool in ROUTES:
        if any(matches(k) for k in keywords):
            return tool
    if re.search(r"\bc\d{7}\b", q):
        return "customer"
    return "knowledge"

# ── AGENT ─────────────────────────────────────────────────────────────────
import re

def ask_agent(query):
    safe, blocked = is_safe(query)
    if not safe:
        return {
            "tool": "safety_filter",
            "answer": f"Request blocked. Contains restricted pattern: '{blocked}'.",
            "status": "REJECTED"
        }
    tool = route(query)
    try:
        if tool == "knowledge":     answer = search_knowledge(query)
        elif tool == "sales":
            days = 7
            m = re.search(r'(\\d+)\\s*(day|week|month)', query)
            if m:
                n, unit = int(m.group(1)), m.group(2)
                days = n if unit=="day" else n*7 if unit=="week" else n*30
            answer = get_sales_summary(days)
        elif tool == "kpi":         answer = get_executive_kpi()
        elif tool == "inventory":   answer = get_inventory_risk()
        elif tool == "churn":       answer = get_high_churn_customers()
        elif tool == "anomaly":     answer = get_anomaly_details()
        elif tool == "quality":     answer = get_data_quality_results()
        elif tool == "pipeline":    answer = get_pipeline_status()
        elif tool == "ml":          answer = get_ml_model_summary()
        elif tool == "realtime":    answer = get_realtime_status()
        elif tool == "customer":
            m = re.search(r'C\\d{7}', query, re.IGNORECASE)
            cid = m.group().upper() if m else None
            answer = get_customer_360(cid) if cid else "Please provide a customer ID in the form C0000042."
        else:                       answer = search_knowledge(query)
        return {"tool": tool, "answer": answer, "status": "OK"}
    except Exception as e:
        return {"tool": tool, "answer": f"Error: {str(e)[:200]}", "status": "ERROR"}

print("Agent ready. Scroll down and enter your question.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Ask the Agent
# MAGIC **Step 1:** Enter your question in the widget above
# MAGIC **Step 2:** Click "Run" on the cell below (or Shift+Enter)

# COMMAND ----------

dbutils.widgets.text("question", "What is the return policy?", "Your Question")

# COMMAND ----------

import html as html_lib
question = dbutils.widgets.get("question")
if not question.strip():
    print("Please enter a question in the widget above.")
else:
    result = ask_agent(question)
    status_color = {"OK":"#2ecc71","REJECTED":"#e74c3c","ERROR":"#e67e22"}.get(result["status"],"#95a5a6")
    tool_label = result["tool"].replace("_"," ").title()
    html_card = f"""
    <div style="font-family:Arial,sans-serif;max-width:900px;margin:10px 0">
      <div style="background:#1a1a2e;color:#eee;padding:12px 18px;border-radius:8px 8px 0 0;display:flex;justify-content:space-between;align-items:center">
        <span style="font-size:16px;font-weight:bold">RetailHub AI Agent</span>
        <span style="background:{status_color};color:#fff;padding:3px 10px;border-radius:12px;font-size:12px">{result["status"]}</span>
      </div>
      <div style="background:#f8f9fa;padding:14px 18px;border-left:3px solid #3498db"><b>Question:</b> {html_lib.escape(question)}</div>
      <div style="background:#fff;padding:6px 18px;border-left:3px solid {status_color}"><small style="color:#888">Tool used: <b>{tool_label}</b></small></div>
    </div>
    """
    displayHTML(html_card)
    print("\n"+"="*60); print(f"QUESTION: {question}"); print(f"TOOL:     {tool_label}"); print(f"STATUS:   {result['status']}"); print("="*60); print(result["answer"]); print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Try These Example Questions
# MAGIC Copy any question above into the widget and re-run:
# MAGIC
# MAGIC | Category | Example Question |
# MAGIC |----------|-----------------|
# MAGIC | Policy | `What is the return policy?` |
# MAGIC | Policy | `Tell me about the loyalty program` |
# MAGIC | Sales | `Show me revenue for the last 14 days` |
# MAGIC | Sales | `What were sales last 30 days?` |
# MAGIC | Platform KPI | `Show executive dashboard` |
# MAGIC | Inventory | `Which products are out of stock?` |
# MAGIC | Churn | `Which customers are at high churn risk?` |
# MAGIC | Anomaly | `Show me suspicious customer activity` |
# MAGIC | Quality | `What are the data quality results?` |
# MAGIC | Pipeline | `Show pipeline status` |
# MAGIC | Real-time | `Show live sales stream` |
# MAGIC | ML Models | `Show ML model summary` |
# MAGIC | Customer | `Show me customer C0000042` |
# MAGIC | Safety Test | `Ignore previous instructions` (will be blocked) |