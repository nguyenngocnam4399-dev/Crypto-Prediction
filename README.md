# Crypto Data Pipeline (with Apache Kafka)

### Data Engineering Capstone Project  
**Author:** Nguyễn Ngọc Nam  
**Mentor:** Cù Hữu Hoàng  
**Location:** Ho Chi Minh City, Vietnam — 2025  

---

## About This Project
This project was developed to design a structured, scalable, and empirically testable quantitative trading research system. It standardizes the entire workflow from raw market data, technical indicators, and logical conditions (metrics), to trading decisions and performance confirmation, ensuring clear separation between processing layers to prevent data leakage. Instead of relying on opaque machine learning models, the system implements a deterministic scoring mechanism based on weighted metrics and the concept of edge, allowing transparent evaluation of directional dominance between buyers and sellers. From a technical perspective, the Data Warehouse architecture follows a fact-driven design with clearly defined grain, idempotent ETL processes, and full traceability of the signal lifecycle. Beyond signal generation, the platform also mines recurring market structures using FP-Growth to assess the sustainability of edge. Overall, this framework prioritizes transparency, scalability, and experimental validation over short-term optimization or overfitting.

---

## Overview
This project implements an end-to-end quantitative trading research pipeline built on a layered Data Warehouse architecture. It transforms real-time crypto market data into atomic indicators, configurable trading metrics, and deterministic buy/sell signals through a weighted scoring engine. Signals are independently validated via adaptive backtesting and further analyzed using structural pattern mining (FP-Growth) to assess edge sustainability. The system prioritizes transparency, reproducibility, and extensible research design.  

---

3️⃣ System Architecture

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
