# Crypto Data Pipeline (with Apache Kafka)

### Data Engineering Capstone Project  
**Author:** Nguyễn Ngọc Nam  
**Mentor:** Cù Hữu Hoàng  
**Location:** Ho Chi Minh City, Vietnam — 2025  

---

## 1️⃣ Introduction
This project was developed to design a structured, scalable, and empirically testable quantitative trading research system. It standardizes the entire workflow from raw market data, technical indicators, and logical conditions (metrics), to trading decisions and performance confirmation, ensuring clear separation between processing layers to prevent data leakage. Instead of relying on opaque machine learning models, the system implements a deterministic scoring mechanism based on weighted metrics and the concept of edge, allowing transparent evaluation of directional dominance between buyers and sellers. From a technical perspective, the Data Warehouse architecture follows a fact-driven design with clearly defined grain, idempotent ETL processes, and full traceability of the signal lifecycle. Beyond signal generation, the platform also mines recurring market structures using FP-Growth to assess the sustainability of edge. Overall, this framework prioritizes transparency, scalability, and experimental validation over short-term optimization or overfitting.

---

## 2️⃣ Overview
This project implements an end-to-end quantitative trading research pipeline built on a layered Data Warehouse architecture. It transforms real-time crypto market data into atomic indicators, configurable trading metrics, and deterministic buy/sell signals through a weighted scoring engine. Signals are independently validated via adaptive backtesting and further analyzed using structural pattern mining (FP-Growth) to assess edge sustainability. The system prioritizes transparency, reproducibility, and extensible research design.  

---

## 3️⃣ System Architecture

![System Architecture](images/System_Architecture.png)

The system follows a layered architecture that clearly separates data ingestion, transformation, signal generation, validation, and analytics. Each layer has a well-defined responsibility to ensure scalability, traceability, and experimental control.

🔹 1. Data Ingestion Layer

Real-time crypto market data is streamed via Kafka.

Spark Streaming processes incoming OHLCV data.

Cleaned data is stored in fact_kline within the Data Warehouse.

This layer ensures immutable, time-series market truth.

🔹 2. Indicator Computation Layer

Atomic technical indicators (RSI, MACD, EMA, ADX, BB, VWAP, etc.) are computed using Spark.

Each indicator is stored independently in fact_indicator.

Indicators are partitioned by symbol and interval to maintain clear grain definition.

This guarantees transparency and recomputability.

🔹 3. Metric Abstraction Layer

Indicators are transformed into logical trading conditions via configurable metrics (dim_metric).

Metrics are evaluated and stored in fact_metric_value.

Logic includes threshold checks, trend direction, cross detection, and volatility filters.

This enables strategy tuning without code modification.

🔹 4. Prediction Engine

BUY and SELL scores are computed independently using weighted metrics.

Edge and confidence are calculated to measure directional dominance.

Signals are stored in fact_prediction.

This layer remains deterministic and fully explainable.

🔹 5. Backtesting & Confirmation Engine

Predictions are evaluated using adaptive TP/SL logic within a future lookahead window.

Results are stored in fact_prediction_result.

Confirmation is separated from prediction to avoid leakage.

This ensures realistic performance validation.

🔹 6. Analytics & Pattern Mining

Performance metrics (equity curve, rolling stability, expectancy) are derived from warehouse facts.

FP-Growth mining extracts recurring structural win patterns.

Flask API exposes analytics endpoints for visualization.

This layer supports experimental research and structural edge validation.

---

## 4️⃣ Data Warehouse Design
Data Warehouse Schema
![Warehouse ERD](images/warehouse_schema.png)
The warehouse follows a fact-driven design where market data, indicators, metrics, predictions, and confirmation results are stored as separate fact tables with clearly defined grain. Dimension tables provide normalization for symbols, intervals, indicators, and metrics.

---

## 🔹 Core Dimensions

- **dim_symbol** – tradable assets  
- **dim_interval** – timeframe definition  
- **dim_indicator_type** – atomic indicator registry  
- **dim_metric** – configurable trading logic  

---

## 🔹 Fact Layers

| Table                     | Grain                                      | Purpose                          |
|---------------------------|--------------------------------------------|----------------------------------|
| `fact_kline`              | (symbol, interval, close_time)             | Raw market data                 |
| `fact_indicator`          | (symbol, interval, indicator, timestamp)   | Atomic indicator values         |
| `fact_metric_value`       | (symbol, interval, metric, calculating_at) | Evaluated trading conditions    |
| `fact_prediction`         | (symbol, interval, predicting_at)          | Deterministic trading decisions |
| `fact_prediction_result`  | (prediction_id)                            | Realized trade outcomes         |

---

### Design Principles

- Clear grain definition  
- No cross-layer coupling  
- Idempotent ETL  
- Independent prediction and confirmation  
- Fully traceable signal lifecycle

---

# 5️⃣ Indicator Engineering

The indicator layer transforms raw market data into atomic, recomputable technical signals. Each indicator is calculated independently using Spark and stored in `fact_indicator` with a clearly defined grain:

(symbol_id, interval_id, indicator_type, timestamp)

Supported indicators include RSI, MACD, EMA, ADX, Bollinger Bands, ATR, VWAP deviation, and volume-based metrics.

---

## Design Principles

- **Atomic Storage** – Each row represents a single indicator value at a single timestamp.  
- **No Composite Features** – Indicators are not mixed or pre-aggregated.  
- **Recomputable** – All indicators can be regenerated from `fact_kline`.  
- **Window-Based Computation** – Rolling and exponential calculations use Spark window functions.  
- **Partitioned Processing** – Computation is distributed by symbol and interval for scalability.  
- **Idempotent Writes** – Unique constraints prevent duplicate calculations.

---

This design ensures transparency, avoids hidden transformations, and allows clean separation between raw technical signals and higher-level trading logic.

---

# 6️⃣ Metric Abstraction Layer

The metric layer converts continuous indicator values into structured, configurable trading conditions. Instead of hardcoding strategy rules in application logic, all trading conditions are defined in `dim_metric` and evaluated dynamically into `fact_metric_value`.

Each metric specifies:

- Anchor indicator (`indicator_type_id`)
- Threshold range (`threshold_start`, `threshold_end`)
- Direction logic (ABOVE, BELOW, BETWEEN, TREND_UP, CROSS_UP, etc.)
- Window size and unit
- Weight
- Active flag

---

## Purpose

- Separate technical signals from trading logic  
- Enable configuration-driven strategy design  
- Allow historical re-evaluation without code changes  
- Support metric-level experimentation  

---

## Data Flow

fact_indicator → metric evaluation → fact_metric_value

Each metric is evaluated per symbol, interval, and timestamp, producing structured conditions (typically binary or weighted values) that are later aggregated in the prediction engine.

---

## Design Principles

- **Config-Driven Logic** – Strategy rules live in the database, not in code  
- **Independent Evaluation** – Metrics are computed separately from scoring  
- **Reproducible** – Historical metric states remain traceable  
- **Extensible** – New trading conditions can be added without refactoring  

---

This abstraction layer provides the structural foundation for deterministic scoring and controlled strategy experimentation.

## Key Features

| Category | Description |
|-----------|-------------|
| **Kafka Integration** | Implements **Producer–Consumer architecture** for real-time message streaming. Crawled data is sent to Kafka topics instead of being stored directly in MySQL. |
| **News Pipeline** | Producer crawls crypto news (Coindesk, NewsBTC) → sends messages to Kafka → Consumer stores to MySQL. |
| **Price Pipeline** | Binance price data is pushed into Kafka topic and consumed for processing. |
| **Indicator Pipeline** | Spark consumes stored data to compute SMA, RSI, and Bollinger Bands. |
| **Workflow Orchestration** | Airflow coordinates Producer → Consumer → Spark → Visualization. |
| **Visualization** | Grafana displays both real-time and processed metrics. |

---

## Project Structure
```text
crypto-pipeline/
├── dags/
│   ├── producer_news.py
│   ├── consumer_news.py
│   ├── producer_prices.py
│   ├── consumer_prices.py
│   └── spark_job.py
├── sql/
│   ├── kline_dim_fact.sql
│   ├── indicator_dim_fact
│   └── news_dim_fact.sql
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

## Technology Stack

| Layer | Tools / Technologies |
|-------|----------------------|
| **Message Streaming** | Apache Kafka |
| **Workflow Orchestration** | Apache Airflow |
| **Data Processing** | Apache Spark |
| **Data Storage** | MySQL (Data Warehouse – Dim–Fact Model) |
| **Data Crawling & NLP** | Python (Requests, BeautifulSoup4, NLTK Vader) |
| **Visualization** | Grafana |
| **Deployment** | Docker, Docker Compose |

---

## Usage Guide
## Running the Pipeline
Below are the steps to execute the full end-to-end data pipeline manually or using Airflow.

Step	Description
1️⃣ Start Producers	Run the producer scripts manually to publish data to Kafka topics:
• producer_news.py → Crawls latest crypto news and sends messages to Kafka topic news_topic.
• producer_prices.py → Collects Binance price data and sends messages to Kafka topic price_topic.
2️⃣ Start Consumers	Run the consumer scripts to read and store data:
• consumer_news.py → Consumes data from news_topic, performs sentiment analysis, and writes results to MySQL.
• consumer_prices.py → Consumes data from price_topic, cleans and stores price information in MySQL.
3️⃣ Run Spark Job	Execute spark_job_1.py (either manually or via Airflow DAG spark_indicator) to compute SMA, RSI, and Bollinger Bands from the processed data.
4️⃣ Visualize in Grafana	Open Grafana to view real-time metrics, technical indicators, and sentiment analytics from the crypto data warehouse.

## Example Outputs

### News Data (`news_fact`)
| id | title | sentiment_score | tag_name | created_date |
|----|--------|-----------------|-----------|---------------|
| 1 | Bitcoin Price Surges | 0.67 | Bitcoin | 2025-09-14 12:00:00 |

### Technical Indicators (`indicator_fact`)
| id | symbol_id | type | value | timestamp |
|----|------------|------|--------|------------|
| 1 | 1 | SMA | 42000.123 | 2025-09-14 12:00:00 |
| 2 | 1 | RSI | 55.67 | 2025-09-14 12:00:00 |

---

## Results

✅ **Real-time streaming** between producer and consumer via Kafka.  
✅ **Fully automated pipeline** orchestrated by Airflow.  
✅ **Spark integration** for large-scale technical analysis.  
✅ **Data warehouse** designed for analytical workloads.  
✅ **Dockerized system** for portable deployment.  
✅ **Grafana dashboards** showing live crypto trends and sentiment.  

---

## Limitations & Future Improvements

| Current Limitation | Future Improvement |
|---------------------|--------------------|
| Batch-based Spark processing | Add **Spark Structured Streaming** for full real-time analytics |
| Limited Kafka topic coverage | Expand topics for multiple crypto pairs and sentiment sources |
| Simple text-based sentiment | Integrate deep learning models (BERT, FinBERT) |
| MySQL scalability | Move to distributed storage like BigQuery or Snowflake |

---

## Dashboard Preview
![Grafana Dashboard Example](images/grafana.png)

---

## Acknowledgments
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Airflow](https://airflow.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [NLTK Vader Sentiment](https://www.nltk.org/_modules/nltk/sentiment/vader.html)
- [Grafana](https://grafana.com/)

---

## License
This project is for **educational and research purposes only**.  
© 2025 Nguyễn Ngọc Nam — Data Engineering Project.
