# RetailHub AI — Free Edition E2E v4.2

End-to-end retail data platform on Databricks Free Edition.

## Notebooks (22 total)

| # | Notebook | Purpose |
|---|----------|---------|
| 00 | 00_START_HERE_FREE_EDITION | Entry point and run guide |
| 01 | 00_setup | Schema, quality rules, UC volume initialization |
| 02 | 01_bronze_ingestion | 326K synthetic rows across 15 Bronze tables |
| 03 | 02_silver_gold_pipeline | Silver facts/dimensions + Gold analytics |
| 04 | 03_data_quality_checks | Metadata-driven quality framework (25+ rules) |
| 05 | 04_ml_training | Churn, segmentation, anomaly models with MLflow |
| 06 | 05_genai_agent | Grounded GenAI agent with 9 tools |
| 07 | 06_realtime_streaming | Near-real-time streaming with AvailableNow |
| 08 | 06B_lakeflow_pipeline_source | Lakeflow pipeline source (pyspark.pipelines) |
| 09 | 07_runtime_validation | End-to-end validation (20+ checks) |
| 10 | AGENT_CHAT | Interactive chat interface |
| 11 | RESULTS_DASHBOARD | Batch and streaming analytics dashboard |
| 12 | Summarise_EachNotebook_Learning | Detailed per-notebook explanations |

## Run Order
00_setup -> 01_bronze -> 02_silver_gold -> 03_dq -> 04_ml -> 05_genai -> 06_streaming -> 07_validation

## Deployed: 2026-09-21
