# Databricks notebook source
# MAGIC %md
# MAGIC # Retail AI Platform — Corrected Step Summary
# MAGIC
# MAGIC ## What is actually implemented
# MAGIC A Databricks retail lakehouse demonstration with Bronze/Silver/Gold, 36 metadata-driven quality rules, MLflow-tracked sklearn models, a grounded deterministic agent, batch monitoring, and near-real-time Structured Streaming.
# MAGIC
# MAGIC ## Data foundation
# MAGIC - 15 baseline Bronze tables
# MAGIC - 326,308 deterministic synthetic rows before streaming
# MAGIC - 10,000 customers, 30,000 orders, 75,000 order items, 1,000 products
# MAGIC - Current-date anchoring so recent dashboards are populated
# MAGIC - Referential consistency across orders/items/reviews/returns
# MAGIC - Signed inventory movements with realistic semantics
# MAGIC
# MAGIC ## Financial correctness
# MAGIC - Order header totals reconcile to line-item gross values.
# MAGIC - Cancelled orders recognize zero revenue.
# MAGIC - Approved return refunds reduce recognized order revenue and are capped so revenue cannot become negative.
# MAGIC - Product KPIs separately aggregate sales, refunds, and reviews, eliminating many-to-many revenue inflation.
# MAGIC
# MAGIC ## ML correctness
# MAGIC - Churn model logs scaler + Random Forest as one Pipeline.
# MAGIC - Segmentation logs scaler + K-Means as one Pipeline.
# MAGIC - Anomaly detection logs scaler + Isolation Forest as one Pipeline.
# MAGIC - K-Means business labels are derived from observed profiles instead of numeric cluster IDs.
# MAGIC - Demand output is a deterministic 90-day baseline; it is not presented as a trained forecasting model.
# MAGIC - All predictive metrics are demonstration-only because the source data and labels are synthetic.
# MAGIC
# MAGIC ## Real-time path
# MAGIC Rate source → Bronze event MERGE → Silver order MERGE → affected minute/channel Gold recomputation → monitoring MERGE.
# MAGIC
# MAGIC The stream is near-real-time micro-batch processing. It is retry-aware and uses a Unity Catalog volume checkpoint. Fresh demo runs generate IDs from timestamp + source offset so they do not collide with old target rows.
# MAGIC
# MAGIC ## Verification status
# MAGIC The archive can be statically checked for JSON integrity, Python syntax, internal table references, and packaging integrity. Spark/Delta/MLflow/Structured Streaming execution still requires the target Databricks runtime.