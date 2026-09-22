# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · Machine Learning + MLflow — Free Edition E2E v4.0
# MAGIC Trains demonstration churn, segmentation, and anomaly models with preprocessing packaged inside each sklearn Pipeline. The notebook uses a per-user MLflow experiment by default, avoiding a dependency on shared workspace folders. The demand output is a deterministic 90-day daily-average baseline; it is not presented as a calibrated forecasting model.

# COMMAND ----------

# DBTITLE 1,ML Setup
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

import uuid, mlflow, mlflow.sklearn
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, silhouette_score
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType

# Enable Arrow optimization for efficient Spark-to-pandas conversion
try:
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
except Exception:
    pass
spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = str(uuid.uuid4())[:8]
_ml_start = datetime.now(timezone.utc).replace(tzinfo=None)
NOW = _ml_start
try:
    _current_user = spark.sql("SELECT current_user() AS u").first()["u"]
except Exception:
    _current_user = "retail-ai-user"
_default_experiment = f"/Users/{_current_user}/retail-ai-free-edition"
dbutils.widgets.text("mlflow_experiment", _default_experiment, "MLflow experiment")
MLFLOW_EXPERIMENT = dbutils.widgets.get("mlflow_experiment").strip() or _default_experiment
mlflow.set_experiment(MLFLOW_EXPERIMENT)

def log_sklearn_model(model, model_name, input_example):
    try:
        return mlflow.sklearn.log_model(model, name=model_name, input_example=input_example)
    except TypeError:
        return mlflow.sklearn.log_model(model, artifact_path=model_name, input_example=input_example)

def positive_class_probability(fitted_pipeline, X):
    probs = fitted_pipeline.predict_proba(X)
    classes = list(fitted_pipeline.named_steps["model"].classes_)
    if 1 in classes: return probs[:, classes.index(1)]
    return np.ones(len(X)) if classes == [1] else np.zeros(len(X))

print(f"Catalog={CAT} | Run={RUN_ID} | MLflow={mlflow.__version__}")

# COMMAND ----------

# DBTITLE 1,Churn Features (No Leakage)
_sql = f"""
SELECT c.customer_id,
       COALESCE(c.tenure_days,0) as tenure_days,
       COALESCE(c.total_orders,0) as total_orders,
       COALESCE(c.total_revenue,0.0) as total_revenue,
       COALESCE(c.avg_order_value,0.0) as avg_order_value,
       COALESCE(c.days_since_last_order,365) as days_since_last_order,
       COALESCE(c.loyalty_points,0) as loyalty_points,
       CASE WHEN c.customer_segment='Premium' THEN 3 WHEN c.customer_segment='VIP' THEN 4
            WHEN c.customer_segment='Standard' THEN 2 ELSE 1 END as segment_score,
       CASE WHEN dc.status='Churned' THEN 1
            WHEN dc.status='Inactive' AND c.days_since_last_order>500 THEN 1 ELSE 0 END as churn_label
FROM {CAT}.retail_gold.customer_360 c
JOIN {CAT}.retail_silver.dim_customers dc ON c.customer_id=dc.customer_id
"""
pdf = spark.sql(_sql).toPandas().fillna(0)
n_classes = pdf['churn_label'].nunique()
print(f"Features: {len(pdf)} rows | churn rate: {pdf['churn_label'].mean():.2%} | classes: {n_classes}")

# CRITICAL FIX: Remove days_since_last_order from features (it leaks the churn label definition)
# Churn label uses days_since_last_order threshold, so it cannot also be a feature
FEAT = ["tenure_days","total_orders","total_revenue","avg_order_value","loyalty_points","segment_score"]
X = pdf[FEAT]
y = pdf["churn_label"].values

# Time-based train/test split: sort by customer_id as proxy for registration order
# In production, use observation window (e.g., behavior through 2025-12) to predict outcome window (2026-01+)
pdf_sorted = pdf.sort_values("customer_id").reset_index(drop=True)
split_idx = int(len(pdf_sorted) * 0.8)
X_train, X_test = pdf_sorted[FEAT].iloc[:split_idx], pdf_sorted[FEAT].iloc[split_idx:]
y_train, y_test = pdf_sorted["churn_label"].iloc[:split_idx].values, pdf_sorted["churn_label"].iloc[split_idx:].values
print(f"Time-based split: train={len(X_train)}, test={len(X_test)}")

# COMMAND ----------

# DBTITLE 1,Churn Model Comparison
# MODEL COMPARISON: Logistic Regression, Random Forest, Gradient Boosting
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import precision_score, recall_score, f1_score, average_precision_score

models_to_compare = [
    ("Logistic Regression", Pipeline([("scale", StandardScaler()), ("model", LogisticRegression(max_iter=500, random_state=42, class_weight="balanced"))])),
    ("Random Forest", Pipeline([("model", RandomForestClassifier(n_estimators=150, max_depth=8, min_samples_leaf=5, random_state=42, class_weight="balanced", n_jobs=-1))])),  # No scaling for tree-based
    ("Gradient Boosting", Pipeline([("model", GradientBoostingClassifier(n_estimators=100, max_depth=5, learning_rate=0.1, random_state=42))]))
]

best_model, best_f1, best_run_id = None, 0.0, None
model_results = []

for model_name, pipeline in models_to_compare:
    with mlflow.start_run(run_name=f"churn_{model_name.replace(' ','_').lower()}_v4") as run:
        pipeline.fit(X_train, y_train)
        y_prob = positive_class_probability(pipeline, X_test)
        y_pred = pipeline.predict(X_test)
        
        # Comprehensive metrics
        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob) if len(np.unique(y_test)) > 1 else 0.0
        pr_auc = average_precision_score(y_test, y_prob) if len(np.unique(y_test)) > 1 else 0.0
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        
        mlflow.log_params({"model_type": model_name, "feature_count": len(FEAT), "train_size": len(X_train), "test_size": len(X_test)})
        mlflow.log_metrics({
            "accuracy": round(acc, 4), "roc_auc": round(auc, 4), "pr_auc": round(pr_auc, 4),
            "precision": round(precision, 4), "recall": round(recall, 4), "f1_score": round(f1, 4)
        })
        log_sklearn_model(pipeline, "churn_pipeline", X_test.head(3))
        
        model_results.append({"model": model_name, "f1": f1, "pr_auc": pr_auc, "roc_auc": auc, "precision": precision, "recall": recall})
        print(f"{model_name}: F1={f1:.4f} | PR-AUC={pr_auc:.4f} | ROC-AUC={auc:.4f} | Precision={precision:.4f} | Recall={recall:.4f}")
        
        if f1 > best_f1:
            best_f1, best_model, best_run_id = f1, pipeline, run.info.run_id

# Select best model based on F1 score
churn_pipeline = best_model
print(f"\n✓ Best model selected based on F1 score: {[r['model'] for r in model_results if r['f1'] == best_f1][0]} (F1={best_f1:.4f})")

# COMMAND ----------

# DBTITLE 1,Customer Segmentation (K-selection)
# K-SELECTION: Test k=2 through k=6, select best based on silhouette score
SEG_FEAT = ["total_revenue","total_orders","days_since_last_order","loyalty_points"]
scaler = StandardScaler()
seg_X_scaled = scaler.fit_transform(pdf[SEG_FEAT])

k_results = []
for k in range(2, 7):
    with mlflow.start_run(run_name=f"segmentation_kmeans_k{k}_v4"):
        km = KMeans(n_clusters=k, random_state=42, n_init=20)
        clusters = km.fit_predict(seg_X_scaled)
        sil = silhouette_score(seg_X_scaled, clusters) if len(pdf) > k else 0.0
        mlflow.log_params({"n_clusters": k, "n_init": 20})
        mlflow.log_metrics({"inertia": round(km.inertia_, 2), "silhouette": round(float(sil), 4)})
        k_results.append({"k": k, "silhouette": sil, "inertia": km.inertia_, "model": km})
        print(f"k={k}: silhouette={sil:.4f}, inertia={km.inertia_:.2f}")

# Select best k based on silhouette score
best_k_result = max(k_results, key=lambda x: x["silhouette"])
best_k = best_k_result["k"]
print(f"\n✓ Best k selected: {best_k} (silhouette={best_k_result['silhouette']:.4f})")

# Train final model with best k
segmentation_pipeline = Pipeline([
    ("scale", StandardScaler()),
    ("model", KMeans(n_clusters=best_k, random_state=42, n_init=20))
])
with mlflow.start_run(run_name=f"segmentation_kmeans_final_k{best_k}_v4") as seg_run:
    pdf["cluster"] = segmentation_pipeline.fit_predict(pdf[SEG_FEAT])
    seg_X = segmentation_pipeline.named_steps["scale"].transform(pdf[SEG_FEAT])
    km = segmentation_pipeline.named_steps["model"]
    sil = silhouette_score(seg_X, pdf["cluster"]) if len(pdf) > best_k else 0.0
    mlflow.log_params({"n_clusters": best_k, "n_init": 20, "selected_by": "max_silhouette"})
    mlflow.log_metrics({"inertia": round(km.inertia_, 2), "silhouette": round(float(sil), 4)})
    log_sklearn_model(segmentation_pipeline, "segmentation_pipeline", pdf[SEG_FEAT].head(3))
    segmentation_run_id = seg_run.info.run_id
    print(f"Final segmentation: k={best_k}, inertia={km.inertia_:.2f}, silhouette={sil:.4f}")

# Assign business labels dynamically based on best_k
profile = (pdf.groupby("cluster")
             .agg(total_revenue=("total_revenue","mean"), total_orders=("total_orders","mean"),
                  recency=("days_since_last_order","mean")))
profile["value_score"] = profile["total_revenue"] + profile["total_orders"]*50 - profile["recency"]*2
ordered_clusters = profile.sort_values("value_score").index.tolist()

# Dynamic label assignment based on k
if best_k == 2:
    label_order = ["Low Value", "High Value"]
elif best_k == 3:
    label_order = ["Low Value", "Medium Value", "High Value"]
elif best_k == 4:
    label_order = ["Low Value", "Developing", "High Value", "Champions"]
elif best_k == 5:
    label_order = ["Low Value", "Developing", "Medium Value", "High Value", "Champions"]
else:  # k=6
    label_order = ["Low Value", "Emerging", "Developing", "Growing", "High Value", "Champions"]

SEG = {cluster: label_order[i] for i, cluster in enumerate(ordered_clusters)}
print(f"Cluster labels (k={best_k}):", SEG)

# COMMAND ----------

anomaly_pipeline = Pipeline([
    ("scale", StandardScaler()),
    ("model", IsolationForest(contamination=0.05, random_state=42, n_estimators=150, n_jobs=-1))
])
with mlflow.start_run(run_name="anomaly_iforest_v3_2"):
    anomaly_pipeline.fit(pdf[FEAT])
    pdf["is_anomaly"] = (anomaly_pipeline.predict(pdf[FEAT])==-1).astype(int)
    pdf["anomaly_score_raw"] = -anomaly_pipeline.decision_function(pdf[FEAT])
    n = int(pdf["is_anomaly"].sum())
    mlflow.log_metrics({"anomaly_count":n,"anomaly_rate_pct":round(n/len(pdf)*100,2)})
    log_sklearn_model(anomaly_pipeline, "anomaly_pipeline", pdf[FEAT].head(3))
    print(f"Anomalies: {n}")

# COMMAND ----------

pdf["churn_probability"] = positive_class_probability(churn_pipeline, pdf[FEAT])
pdf["churn_predicted"] = (pdf["churn_probability"]>=0.5).astype(int)
(spark.createDataFrame(pdf[["customer_id","churn_probability","churn_predicted","churn_label","cluster","anomaly_score_raw","is_anomaly"]]
                       .rename(columns={"churn_label":"actual_churn"}))
 .withColumn("model_version",lit("v3.2")).withColumn("scored_at",lit(NOW).cast("timestamp"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .saveAsTable(f"{CAT}.retail_ml.churn_predictions"))
print(f"churn_predictions: {spark.table(f'{CAT}.retail_ml.churn_predictions').count()}")

# COMMAND ----------

# DBTITLE 1,ML Segments
seg_udf = udf(lambda x: SEG.get(int(x),"Unknown"), StringType())
(spark.createDataFrame(pdf[["customer_id","cluster"]].rename(columns={"cluster":"segment_id"}))
 .withColumn("segment_label", seg_udf("segment_id"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .saveAsTable(f"{CAT}.retail_ml.customer_segments"))
print(f"customer_segments: {spark.table(f'{CAT}.retail_ml.customer_segments').count()}")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_ml.customer_segments IS 'KMeans customer segmentation: segment_id and label per customer'")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_ml.churn_predictions IS 'Churn model predictions: probability, predicted label, actual label, cluster, and anomaly flag per customer'")

# COMMAND ----------

# DBTITLE 1,Anomaly & Demand
(spark.createDataFrame(pdf[["customer_id","anomaly_score_raw","is_anomaly"]])
 .write.format("delta").mode("overwrite").option("overwriteSchema","true")
 .saveAsTable(f"{CAT}.retail_ml.anomaly_scores"))

spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_ml.demand_forecast AS
WITH daily AS (
  SELECT i.product_id, o.order_date, SUM(i.quantity) AS daily_units
  FROM {CAT}.retail_silver.fact_order_items i JOIN {CAT}.retail_silver.fact_orders o ON i.order_id=o.order_id
  WHERE o.order_date >= date_sub(current_date(),89) AND o.order_status <> 'Cancelled'
  GROUP BY i.product_id, o.order_date
), stats AS (
  SELECT product_id, SUM(daily_units) AS units_90d, AVG(daily_units) AS avg_active_day_units,
         STDDEV_POP(daily_units) AS std_active_day_units, COUNT(DISTINCT order_date) AS active_days
  FROM daily GROUP BY product_id
)
SELECT p.product_id, 'next_30d_baseline' AS forecast_period,
       CAST(ROUND(COALESCE(s.units_90d,0)/90.0*30,0) AS BIGINT) AS predicted_units,
       CASE WHEN COALESCE(s.active_days,0)=0 THEN 0.0 ELSE ROUND(LEAST(0.95,
         COALESCE(s.active_days,0)/90.0 * (1.0/(1.0+COALESCE(s.std_active_day_units,0)/(COALESCE(s.avg_active_day_units,0)+1.0)))),3) END AS stability_score,
       '90d_calendar_daily_average_baseline' AS forecast_method, current_timestamp() AS generated_at
FROM {CAT}.retail_silver.dim_products p LEFT JOIN stats s ON p.product_id=s.product_id
""")
print(f"anomaly_scores: {spark.table(f'{CAT}.retail_ml.anomaly_scores').count()}, demand_baseline: {spark.table(f'{CAT}.retail_ml.demand_forecast').count()}")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_ml.anomaly_scores IS 'Isolation Forest anomaly scores: raw anomaly score and binary is_anomaly flag per customer'")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_ml.demand_forecast IS 'Deterministic 90-day daily-average demand baseline per product with stability score'")

# COMMAND ----------

# DBTITLE 1,ML Pipeline Log & Registration
spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.anomaly_summary AS SELECT is_anomaly, COUNT(*) as count, round(AVG(churn_probability),4) as avg_churn FROM {CAT}.retail_ml.churn_predictions GROUP BY is_anomaly")
spark.sql(f"CREATE OR REPLACE TABLE {CAT}.retail_gold.anomaly_kpi AS SELECT current_date() as report_date, COUNT(*) as total_scored, SUM(is_anomaly) as anomaly_count, round(SUM(is_anomaly)*100.0/COUNT(*),2) as anomaly_rate_pct, round(AVG(churn_probability),4) as avg_churn_risk FROM {CAT}.retail_ml.churn_predictions")
# CRITICAL FIX: Base anomaly severity on anomaly_score (not churn_probability)
spark.sql(f"""
CREATE OR REPLACE TABLE {CAT}.retail_monitoring.anomaly_events AS
SELECT concat('ANM',cast(row_number() OVER (ORDER BY anomaly_score_raw DESC) as string)) as event_id,
       customer_id as entity_id, 'customer' as entity_type, 'customer_behavior' as anomaly_type,
       CASE WHEN anomaly_score_raw >= PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY anomaly_score_raw) OVER () THEN 'critical'
            WHEN anomaly_score_raw >= PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY anomaly_score_raw) OVER () THEN 'high'
            ELSE 'medium' END as severity,
       anomaly_score_raw as anomaly_score, churn_probability, current_timestamp() as detected_at
FROM {CAT}.retail_ml.churn_predictions
WHERE is_anomaly=1
ORDER BY anomaly_score_raw DESC LIMIT 500
""")
scored_rows = spark.table(f"{CAT}.retail_ml.churn_predictions").count()

# Add governance comments for ML output tables
spark.sql(f"COMMENT ON TABLE {CAT}.retail_gold.anomaly_summary IS 'Anomaly summary: count and avg churn probability grouped by is_anomaly flag'")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_gold.anomaly_kpi IS 'Anomaly KPI: total scored, anomaly count, rate, and avg churn risk'")
spark.sql(f"COMMENT ON TABLE {CAT}.retail_monitoring.anomaly_events IS 'Top 500 anomaly events by score with severity tier and churn probability'")

# Register trained models in the Unity Catalog Model Registry using captured run IDs
try:
    if best_run_id:
        mlflow.register_model(f"runs:/{best_run_id}/churn_pipeline", f"{CAT}.retail_ml.churn_model")
        print(f"✓ Churn model registered to {CAT}.retail_ml.churn_model (run: {best_run_id[:8]})")
    if segmentation_run_id:
        mlflow.register_model(f"runs:/{segmentation_run_id}/segmentation_pipeline", f"{CAT}.retail_ml.segmentation_model")
        print(f"✓ Segmentation model registered to {CAT}.retail_ml.segmentation_model (run: {segmentation_run_id[:8]})")
except Exception as e:
    print(f"⚠ Model registration skipped: {str(e)[:200]}")

spark.sql(f"""
INSERT INTO {CAT}.retail_monitoring.pipeline_runs
VALUES ('{RUN_ID}','04_ml_training','ml','SUCCEEDED',{scored_rows},timestamp('{_ml_start}'),current_timestamp(),{round((datetime.now(timezone.utc).replace(tzinfo=None) - _ml_start).total_seconds(), 2)},'{PROJECT_VERSION}','free-edition','')
""")
print("ML complete.")
