# Deterministic Quantitative Trading Research Infrastructure  
### Scalable Signal Modeling & Structural Edge Validation Platform  

**Author:** Nguyễn Ngọc Nam  
**Mentor:** 
Ho Chi Minh City, Vietnam — 2025  

---

# 1. Introduction

This project implements a deterministic quantitative trading research infrastructure designed to model, validate, and analyze trading signals under strict leakage control.

The system separates signal generation from confirmation, applies weighted metric-based edge modeling, and stores every stage of the signal lifecycle in a fact-driven Data Warehouse to ensure full reproducibility and auditability.

Rather than relying on opaque machine learning models, this framework emphasizes explainable signal construction, controlled backtesting, and structural edge validation. The architecture prioritizes transparency, scalability, and disciplined experimentation over short-term optimization.

---

# 2. System Outputs & User Value

This platform delivers structured trading intelligence across multiple layers:

## Explainable Trading Signals
- Deterministic BUY / SELL decisions  
- Edge and confidence scoring  
- Full transparency of contributing metrics  
- No black-box logic  

## Controlled Performance Evaluation
- Leakage-safe confirmation framework  
- Adaptive TP/SL risk modeling  
- Expectancy and rolling stability analysis  
- Equity curve and drawdown evaluation  

## Structural Market Insights
- Regime-based performance segmentation  
- FP-Growth structural pattern mining  
- Lift-based edge validation  
- Identification of recurring market conditions  

## Scalable Research Infrastructure
- Warehouse-driven experimentation  
- Configurable metric logic  
- DAG-based orchestration via Airflow  
- Fully reproducible signal lifecycle  

The system transforms raw market data into structured, auditable trading intelligence.

---

# 3. System Architecture

![System Architecture](images/System_Architecture.png)

The platform follows a layered architecture separating ingestion, transformation, signal modeling, orchestration, validation, and analytics.

---

## Data Ingestion

- Kafka streams real-time OHLCV market data  
- Spark Streaming processes and normalizes records  
- Clean data stored in `fact_kline`  

Establishes immutable time-series market truth.

---

## Indicator Computation

- Atomic indicators computed via Spark  
- Stored independently in `fact_indicator`  
- Partitioned by symbol and interval  
- Window-based distributed computation  

Ensures transparency and recomputability.

---

## Metric Abstraction

- Trading conditions defined in `dim_metric`  
- Evaluated into `fact_metric_value`  
- Supports threshold, trend, cross, and volatility logic  
- Fully config-driven (no hardcoded rules)  

---

## Prediction Engine

BUY and SELL signals are scored independently:

buy_score  = Σ(weighted BUY metrics)  
sell_score = Σ(weighted SELL metrics)  

edge = |buy_score − sell_score|  
confidence = max(score) / MAX_SCORE  

Includes dominance thresholding, conflict detection, and edge gating.  
Results are stored in `fact_prediction`.

---

## Backtesting & Confirmation

- Adaptive TP/SL within controlled lookahead window  
- Strict separation from prediction layer  
- Idempotent result writing  
- Leakage prevention  

Results stored in `fact_prediction_result`.

---

## Orchestration Layer (Apache Airflow)

The workflow is coordinated using Apache Airflow DAGs running in a Linux environment.

- Dependency-controlled Spark job execution  
- Scheduled metric, prediction, and confirmation tasks  
- Retry and failure handling  
- CeleryExecutor for distributed execution  

Ensures reproducible, production-grade orchestration.

---

## Analytics & Pattern Mining

- Equity curve, expectancy, drawdown  
- Rolling stability metrics  
- Regime-based segmentation  
- FP-Growth structural mining  
- Flask API for analytics endpoints  

---

# 4. Data Warehouse Design

![Warehouse ERD](images/warehouse_schema.png)

The warehouse follows a fact-driven layered model with explicit grain definition to preserve traceability and reproducibility.

---

## Core Dimensions

- `dim_symbol`  
- `dim_interval`  
- `dim_indicator_type`  
- `dim_metric`  

---

## Fact Tables

| Table                    | Grain                                      | Purpose |
|--------------------------|--------------------------------------------|---------|
| `fact_kline`             | (symbol, interval, close_time)             | Market data |
| `fact_indicator`         | (symbol, interval, indicator, timestamp)   | Atomic signals |
| `fact_metric_value`      | (symbol, interval, metric, calculating_at) | Logical conditions |
| `fact_prediction`        | (symbol, interval, predicting_at)          | Trading hypothesis |
| `fact_prediction_result` | (prediction_id)                            | Realized outcome |

---

## Design Principles

- Explicit grain control  
- Idempotent ETL  
- Strict layer separation  
- Prediction/confirmation decoupling  
- Full signal lifecycle traceability  

---

# 5. Indicator Engineering

Indicators are stored at atomic granularity:

(symbol_id, interval_id, indicator_type, timestamp)

Key characteristics:

- Spark window-based computation  
- Partitioned distributed processing  
- No composite pre-aggregation  
- Fully recomputable from raw kline  

---

# 6. Metric Abstraction Layer

Metrics convert continuous indicator values into structured trading conditions.

Each metric defines:

- Anchor indicator  
- Threshold range  
- Direction rule  
- Window size  
- Weight  

Evaluated into `fact_metric_value` and later aggregated by the prediction engine.

---

# 7. Backtesting & Risk Framework

- Controlled lookahead window  
- Adaptive TP/SL scaling with edge strength  
- Idempotent confirmation logic  
- No double counting  

Designed to minimize leakage and overfitting bias.

---

# 8. Deployment Environment

The system is deployed on a Linux-based environment with:

- Apache Kafka (streaming layer)  
- Apache Spark (distributed processing)  
- Apache Airflow (workflow orchestration)  
- MySQL (Data Warehouse)  
- Flask (analytics API)  

Services are configured and managed directly at the OS level without containerization.

---

# 9. Project Structure

```text
crypto-quant-platform/
│
├── dags/                     # Airflow DAG definitions
├── kafka/                    # Producer / Consumer modules
├── spark_jobs/               # Spark processing jobs
├── indicators/               # Indicator computation modules
├── sql/                      # Dimension & fact table schemas
├── app/                      # Flask analytics API
├── config/                   # Configuration files
├── requirements.txt
└── README.md
