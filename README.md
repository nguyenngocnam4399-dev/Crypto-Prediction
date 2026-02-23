# 🚀 Deterministic Quantitative Trading Research Platform  
### Hệ Thống Phân Tích & Dự Đoán Tài Sản Số Dựa Trên Dữ Liệu Thời Gian Thực  

**Author:** Nguyễn Ngọc Nam  
**Mentor:** Phạm Long Vân - Data Manager  
**Location:** Ho Chi Minh City, Vietnam — 2025  

---

# 1️⃣ Introduction

Trong bối cảnh tài sản số (Crypto Assets) ngày càng phát triển mạnh mẽ, đặc biệt là các đồng có tính thanh khoản cao như BTC, ETH, BNB, số lượng nhà đầu tư cá nhân và tổ chức tham gia thị trường ngày càng tăng.
Song song với đó, xu hướng quản lý và cấp phép tài sản số tại nhiều quốc gia (bao gồm Việt Nam trong tương lai gần) khiến nhu cầu:
Phân tích dữ liệu thị trường chuyên sâu
Đánh giá rủi ro đầu tư
Dự đoán xu hướng giá
Kiểm định tính ổn định của tín hiệu giao dịch
trở nên cấp thiết.
Tuy nhiên, phần lớn nhà đầu tư hiện nay:
Dựa vào cảm tính hoặc tin tức rời rạc
Thiếu hệ thống phân tích dữ liệu có cấu trúc
Không có cơ chế kiểm chứng tín hiệu trước khi ra quyết định
Vì vậy, dự án này được xây dựng nhằm:
Xây dựng một hệ thống phân tích và dự đoán tài sản số dựa trên dữ liệu thời gian thực, có khả năng hỗ trợ quyết định đầu tư với độ chính xác tương đối tốt, áp dụng được cho cả người phát triển và người dùng cuối (end-user).
Hệ thống tập trung vào các tài sản có giá trị và tính ổn định cao:
BTC
ETH
BNB

---

# 2️⃣ System Outputs & User Value

This platform delivers multiple layers of value:

## 1. Explainable Trading Signals
- Deterministic BUY / SELL decisions  
- Edge and confidence scoring  
- Transparent metric contribution  
- No black-box logic  

## 2. Controlled Performance Evaluation
- Leakage-safe confirmation framework  
- Adaptive TP/SL risk modeling  
- Expectancy & drawdown analysis  
- Equity curve simulation  

## 3. Structural Market Insights
- Regime-based segmentation  
- FP-Growth structural mining  
- Lift-based validation  
- Recurring condition detection  

## 4. Integrated Market + News Intelligence
- Real-time sentiment ingestion  
- Weighted symbol-level sentiment modeling  
- Window-based aggregation  
- Deterministic integration into scoring  

---

# 3️⃣ System Architecture

![System Architecture](images/System_Architecture.png)

The system follows a layered architecture separating ingestion, transformation, signal modeling, orchestration, validation, and analytics.

---

## 1. Data Ingestion

### Market Data
- Kafka streams real-time OHLCV data  
- Spark Streaming normalizes records  
- Stored in `fact_kline`  

### News Data
- News crawled from crypto media sources  
- Sent to Kafka topic  
- Consumed and stored in `news_fact`  
- Symbol mapping stored in `news_coin_fact`  

Both streams remain independent and immutable.

---

## 2. Indicator Computation

- Atomic indicators computed via Spark  
- Stored in `fact_indicator`  
- Partitioned by symbol and interval  
- Fully recomputable from raw kline  

---

## 3. News Sentiment Processing

News sentiment is modeled as a multi-layer fact pipeline:

### Raw Layer — `news_fact`
Grain: 1 row = 1 article  
- title  
- url (UNIQUE)  
- sentiment_score  
- created_date  
- view_number  
- tag_id  

### Mapping Layer — `news_coin_fact`
Grain: `(news_id, symbol_id)`  
- symbol attribution  
- confidence score  

### Weighted Layer — `news_sentiment_weighted_fact`
Grain: `(news_id, symbol_id)`  

Includes:
- raw_sentiment  
- tag_weight  
- confidence  
- weighted_score  
- final_score  
- event_time  

Constraints:
- UNIQUE(news_id, symbol_id)  
- Indexed for join optimization  

### Aggregated Layer — `news_sentiment_agg_fact`
Grain: `(symbol_id, window_start)`  

- news_count  
- sentiment_weighted  

Aligned to trading interval resolution.

---

## 4. Metric Abstraction

- Trading conditions defined in `dim_metric`  
- Technical + sentiment metrics supported  
- Evaluated into `fact_metric_value`  
- Threshold, trend, cross, volatility logic  

---

## 5. Prediction Engine

buy_score  = Σ(weighted BUY metrics)  
sell_score = Σ(weighted SELL metrics)  

edge = |buy_score − sell_score|  
confidence = max(score) / MAX_SCORE  

Stored in `fact_prediction`.

Deterministic, explainable, leakage-safe.

---

## 6. Backtesting & Confirmation

- Adaptive TP/SL  
- Controlled lookahead  
- Results stored in `fact_prediction_result`  
- Strict separation from prediction  

---

## 7. Orchestration Layer (Apache Airflow)

- DAG-based workflow control  
- Spark job scheduling  
- Metric & sentiment pipelines  
- Retry & failure handling  
- CeleryExecutor for distributed tasks  

Runs on Linux-based infrastructure.

---

## 8. Analytics & Pattern Mining

- Win Rate  
- Expectancy  
- Rolling stability  
- Equity curve  
- Drawdown  
- FP-Growth structural validation  

---

# 4️⃣ Data Warehouse Design

![Warehouse ERD](images/warehouse_schema.png)

Fact-driven layered warehouse with explicit grain definition.

## Core Dimensions
- `dim_symbol`
- `dim_interval`
- `dim_indicator_type`
- `dim_metric`

## Market Fact Tables

| Table | Grain | Role |
|-------|-------|------|
| `fact_kline` | (symbol, interval, close_time) | Market data |
| `fact_indicator` | (symbol, interval, indicator, timestamp) | Atomic signals |
| `fact_metric_value` | (symbol, interval, metric, calculating_at) | Conditions |
| `fact_prediction` | (symbol, interval, predicting_at) | Hypothesis |
| `fact_prediction_result` | (prediction_id) | Realized outcome |

## News Fact Tables

| Table | Grain | Role |
|-------|-------|------|
| `news_fact` | (id) | Raw articles |
| `news_coin_fact` | (news_id, symbol_id) | Symbol mapping |
| `news_sentiment_weighted_fact` | (news_id, symbol_id) | Weighted sentiment |
| `news_sentiment_agg_fact` | (symbol_id, window_start) | Aggregated sentiment |

Design Principles:
- Explicit grain  
- Idempotent ETL  
- Event-time alignment  
- No cross-layer coupling  
- Fully traceable lifecycle  

---

# 🔟 Tech Stack & Engineering Practices

## Tech Stack

- Python  
- PySpark  
- Apache Kafka  
- Apache Airflow (CeleryExecutor)  
- Spark ML (FPGrowth)  
- MySQL 8  
- Flask  
- NumPy / Pandas  

## Deployment Environment

Linux-based infrastructure:

- Kafka service  
- Spark cluster  
- Airflow scheduler + workers  
- MySQL server  
- Flask API  

No containerization.

---

## Engineering Practices

- Layered architecture  
- Explicit grain control  
- Fact-driven warehouse modeling  
- Idempotent ETL  
- UTC normalization  
- Event-time alignment  
- Config-driven strategy logic  
- Strict prediction/confirmation separation  
- DAG-based orchestration  

---

# 11️⃣ Conclusion

This project establishes a deterministic quantitative research infrastructure grounded in structured data modeling and strict leakage control.

By decoupling ingestion (market & news), signal construction, confirmation, and evaluation into independent layers, the system enforces reproducibility, auditability, and disciplined experimentation. Every stage of the signal lifecycle—technical and sentiment—is persisted at explicit grain within a fact-driven warehouse.

The architecture is designed as a scalable research foundation capable of extending toward:

- Portfolio-level allocation models  
- Transaction cost integration  
- Regime-adaptive weighting  
- Hybrid deterministic–statistical extensions  

The objective is not short-term optimization, but the construction of a controlled environment where edge can be measured, validated, and stress-tested under reproducible conditions.

---

