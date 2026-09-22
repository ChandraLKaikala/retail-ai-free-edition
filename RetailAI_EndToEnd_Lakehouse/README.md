# RetailAI End-to-End Lakehouse

An end-to-end retail lakehouse project built for **Databricks Free Edition**, demonstrating data engineering, data quality, analytics, machine learning, GenAI, streaming, and runtime validation in one integrated workflow.

> **Project version:** 4.0 Free Edition E2E (2026-09-21)

## What This Project Demonstrates

This project follows a practical retail data lifecycle:

```text
Synthetic Retail Data
        |
        v
   Bronze Layer
        |
        v
   Silver Layer
        |
        v
    Gold Layer
        |
        +-------------------+-------------------+
        |                   |                   |
        v                   v                   v
 Data Quality          ML / MLflow         GenAI Agent
        |                                       |
        +-------------------+-------------------+
                            |
                            v
                 Realtime / Lakeflow
                            |
                            v
                  Runtime Validation
```

The implementation uses Unity Catalog objects, Delta tables, PySpark, MLflow, Structured Streaming with `Trigger.AvailableNow()`, and an optional Lakeflow pipeline source.

## Repository Contents

The current project folder contains **13 Databricks Python notebook source files** plus this README.

| Order | File | Purpose |
|---|---|---|
| Start | `00_START_HERE_FREE_EDITION.py` | Project entry point, platform boundaries, architecture, and manual run guidance |
| 1 | `00_setup.py` | Creates governed schemas, metadata-driven quality rules, monitoring objects, and the Unity Catalog volume |
| 2 | `01_bronze_ingestion.py` | Generates deterministic synthetic retail data and writes 326,308 rows across 15 Bronze Delta tables |
| 3 | `02_silver_gold_pipeline.py` | Builds typed Silver facts/dimensions and business-facing Gold analytics |
| 4 | `03_data_quality_checks.py` | Executes the metadata-driven data-quality framework and records results/history |
| 5 | `04_ml_training.py` | Trains retail ML workloads and tracks model experiments with MLflow |
| 6 | `05_genai_agent.py` | Implements a grounded retail assistant with tool routing, knowledge retrieval, SQL guardrails, safety controls, and evaluation |
| 7 | `06_realtime_streaming.py` | Simulates incremental order events and processes them with Structured Streaming `AvailableNow` |
| Optional | `06B_lakeflow_pipeline_source.py` | Lakeflow pipeline source using `pyspark.pipelines`; intended for pipeline context rather than normal notebook execution |
| 8 | `07_runtime_validation.py` | Performs end-to-end checks across data, quality, ML, GenAI, streaming, and Unity Catalog resources |
| Reference | `08_DAB_DEPLOYMENT_GUIDE.py` | Explains deployment through a companion Databricks Declarative Automation Bundle (DAB) |
| Interactive | `AGENT_CHAT.py` | Notebook-based interactive interface for querying the grounded retail agent |
| Dashboard | `RESULTS_DASHBOARD.py` | Results dashboard covering executive KPIs, sales, Customer 360, ML outputs, inventory, data quality, agent evaluation, and realtime/streaming metrics |

## Recommended Manual Run Order

Run the core notebooks in this sequence:

```text
00_setup
   ↓
01_bronze_ingestion
   ↓
02_silver_gold_pipeline
   ↓
03_data_quality_checks
   ↓
04_ml_training
   ↓
05_genai_agent
   ↓
06_realtime_streaming
   ↓
07_runtime_validation
```

Start with `00_START_HERE_FREE_EDITION.py` for the project walkthrough. After the core pipeline is initialized, use `RESULTS_DASHBOARD.py` to review batch, ML, quality, agent, and realtime results, and use `AGENT_CHAT.py` to interact with the retail agent.

Do **not** execute `06B_lakeflow_pipeline_source.py` as a normal notebook. It imports `pyspark.pipelines` and is designed to run in Lakeflow pipeline context.

## Architecture

### Bronze — Raw / Source-Aligned Data

`01_bronze_ingestion.py` creates a deterministic synthetic retail dataset covering customers, products, orders, order items, payments, shipments, returns, inventory, reviews, and other retail domains.

The generator is designed to maintain business consistency such as valid order/customer relationships, coherent payment states, realistic returns, inventory constraints, and purchase-linked reviews.

### Silver — Curated Data

`02_silver_gold_pipeline.py` converts Bronze data into typed, reusable facts and dimensions suitable for downstream analytics and modeling.

### Gold — Business Analytics

The Gold layer produces business-facing datasets such as customer and product analytics. Revenue calculations are refund-aware, cancelled orders recognize zero revenue, and aggregations are structured to avoid many-to-many inflation.

### Data Quality

`00_setup.py` defines a metadata-driven quality-rule table covering checks such as:

- uniqueness and completeness
- referential integrity
- value validity
- temporal consistency
- financial reconciliation
- order/payment/return consistency

`03_data_quality_checks.py` executes those rules and records their outcomes for monitoring.

### Machine Learning & MLflow

`04_ml_training.py` demonstrates retail-oriented ML workflows including customer churn, segmentation, anomaly detection, demand-related modeling, and MLflow experiment tracking.

### GenAI / Agent Layer

`05_genai_agent.py` implements a grounded retail assistant that operates over project data and knowledge. The design includes tool routing, knowledge retrieval, guarded read-only SQL behavior, safety filtering, and evaluation.

`AGENT_CHAT.py` provides an interactive notebook interface for questions such as sales summaries, customer insights, inventory risk, data-quality status, churn risk, and project knowledge.

### Realtime Processing

`06_realtime_streaming.py` uses a Unity Catalog Volume as the landing location for finite incremental event batches. Structured Streaming consumes newly available files with `Trigger.AvailableNow()`, updates Bronze/Silver realtime data, and maintains minute-level Gold sales metrics.

This approach provides a repeatable streaming demonstration without requiring an indefinitely running notebook stream.

### Lakeflow Pipeline

`06B_lakeflow_pipeline_source.py` provides an optional managed Lakeflow implementation with:

- Auto Loader ingestion
- streaming Bronze data
- data-quality expectations
- streaming Silver transformations
- a Gold minute-level materialized view

It is intended to be executed by Lakeflow rather than manually.

### Results Dashboard

`RESULTS_DASHBOARD.py` provides a consolidated notebook dashboard over the project outputs. It includes executive KPIs, recent revenue by channel, Customer 360, churn-risk distribution, customer segments, anomaly results, demand baseline, inventory health, data-quality results, agent evaluation/interactions, live sales, and streaming health. Run the numbered pipeline notebooks first; run `06_realtime_streaming.py` before expecting live streaming views.

### Runtime Validation

`07_runtime_validation.py` verifies that the major components connect correctly. Validation includes object existence, referential and financial consistency, data-quality execution, ML coverage, GenAI evaluation, streaming output, and the governed Unity Catalog volume.

## Databricks Free Edition Design

The project intentionally uses patterns suitable for the Free Edition implementation represented by these notebooks:

- Python / PySpark notebook sources
- Unity Catalog schemas, managed Delta tables, and a managed Volume
- serverless-oriented execution
- no dependency on DBFS root for streaming landing/checkpoint paths
- finite `AvailableNow` streaming runs
- no external API requirement for the generated dataset and local grounded-agent workflow

## Getting Started

1. Import or clone the project into a Databricks workspace.
2. Open `00_START_HERE_FREE_EDITION.py`.
3. Run `00_setup.py` and select the target catalog when prompted.
4. Execute the core notebooks in the recommended order above.
5. Run `07_runtime_validation.py` to verify the end-to-end implementation.
6. Open `RESULTS_DASHBOARD.py` to review project results.
7. Open `AGENT_CHAT.py` to explore the agent interactively.

The notebooks use a catalog widget and default to the current catalog when available.

## Key Technologies

**Databricks · Apache Spark / PySpark · Delta Lake · Unity Catalog · Structured Streaming · Lakeflow · MLflow · Machine Learning · GenAI · Data Quality · Medallion Architecture**

## Scope

This repository is a portfolio/demo implementation intended to demonstrate how multiple data and AI workloads can be connected into a coherent retail lakehouse architecture. Synthetic data is used so the project can be explored without external source systems or proprietary datasets.

## Project Structure

```text
RetailAI_EndToEnd_Lakehouse/
├── 00_START_HERE_FREE_EDITION.py
├── 00_setup.py
├── 01_bronze_ingestion.py
├── 02_silver_gold_pipeline.py
├── 03_data_quality_checks.py
├── 04_ml_training.py
├── 05_genai_agent.py
├── 06_realtime_streaming.py
├── 06B_lakeflow_pipeline_source.py
├── 07_runtime_validation.py
├── 08_DAB_DEPLOYMENT_GUIDE.py
├── AGENT_CHAT.py
├── RESULTS_DASHBOARD.py
└── README.md
```

---

Built as an end-to-end demonstration of modern retail data engineering, analytics, ML, GenAI, and realtime processing on Databricks.
