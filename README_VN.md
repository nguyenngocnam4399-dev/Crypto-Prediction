# 🚀 Production-Grade Quantitative Crypto Research Platform

> Real-time Data Engineering Pipeline & Quantitative Trading Research System  
> Focus Assets: BTC • ETH • BNB

---

## 📌 Executive Summary

This project is a **production-oriented data engineering and quantitative research platform** designed to:

- Ingest real-time cryptocurrency market data
- Standardize and warehouse structured financial data
- Generate deterministic trading signals
- Backtest and evaluate performance stability
- Extract statistically significant trading patterns
- Deliver analytics through a reporting dashboard

The system is engineered with **scalability, idempotency, fault tolerance, and analytical traceability** in mind.

---

# 1️⃣ Business Context & Motivation

Digital assets are becoming a regulated and institutionalized financial class.  
As liquidity and participation increase, decision-making must shift from intuition to quantitative frameworks.

Key challenges:

- High market volatility
- Noise-driven sentiment cycles
- Lack of structured retail-level analytics
- Absence of reproducible trading logic

This platform addresses those challenges by:

- Building a deterministic scoring engine
- Applying statistical validation (backtest, expectancy, regression)
- Mining repeatable winning patterns
- Delivering explainable signal outputs

---

# 2️⃣ High-Level Architecture

## System Architecture

![System Architecture](images/System_Architecture.png)

The architecture follows a layered design:

### 🔹 Data Ingestion Layer
- Binance WebSocket / API
- News Crawlers
- Kafka Streaming

### 🔹 Processing Layer
- Spark (Batch + Stream)
- Indicator Computation Engine
- Metric & Scoring Engine
- Backtest Engine
- FP-Growth Pattern Mining

### 🔹 Storage Layer
- MySQL Data Warehouse (Dim-Fact Modeling)

### 🔹 Orchestration Layer
- Airflow DAG scheduling
- Retry & failure handling
- Idempotent execution

### 🔹 Presentation Layer
- Flask API
- Analytics Dashboard

---

# 3️⃣ Data Engineering Design

## 🔄 Real-Time Ingestion

- Kafka decouples producer & consumer
- Enables replay & horizontal scaling
- Handles streaming volatility bursts

## ⚡ Distributed Processing

Apache Spark is used for:

- Indicator calculation (RSI, MACD, EMA, BB, ADX, VWAP, ATR, OBV)
- Metric evaluation
- Deterministic signal scoring
- Backtest confirmation logic
- Pattern mining preparation

Processing design ensures:

- Partition-aware aggregation
- Idempotent writes
- Anti-duplicate insertion logic
- Symbol-isolated computation

---

# 4️⃣ Data Warehouse Architecture

## Dim-Fact Modeling

![Warehouse Schema](images/warehouse_schema_crypto.png)

![News Warehouse Schema](images/warehouse_schema_news.png)

### Dimension Tables
- `dim_symbol`
- `dim_interval`
- `dim_indicator_type`
- `dim_metric`
- `tag_dim`

### Fact Tables
- `fact_kline`
- `fact_indicator`
- `fact_metric_value`
- `fact_prediction`
- `fact_prediction_result`
- `news_sentiment_weighted_fact`
- `fp_growth_win_patterns`
- `fp_growth_win_rules`

---

## Why Dim-Fact?

- Historical traceability
- Query performance optimization
- Clean separation of context vs events
- Scalable metric expansion
- Compatible with DW best practices

---

# 5️⃣ Signal Modeling Framework

## 🧮 Deterministic Market Scoring

Market Score =  
Trend + Momentum + Volume + Volatility

Confidence Score =  
Market Score / Max Score

### Guard Mechanisms

- Conflict Detection
- Weak Edge Filter
- Confidence Band Filter
- No-Trade Flag Logic

The design prevents:

- Overtrading
- High-variance regime breakdown
- False positives during squeeze conditions

---

# 6️⃣ Backtest & Risk Modeling

Backtest Engine evaluates:

- Dynamic TP/SL logic
- Lookahead window evaluation
- Win/Loss classification
- PnL normalization
- Rolling expectancy
- Regime-dependent performance

This ensures:

- Survivability validation
- Edge persistence testing
- Overfitting detection

---

# 7️⃣ Advanced Analytics Layer

## 📈 Equity Curve & Drawdown

![Equity Curve](images/equity_curve.png)

Measures:
- Capital growth
- Maximum drawdown
- Risk-adjusted survivability

---

## 📉 Rolling Expectancy

![Rolling Expectancy](images/rolling_expectancy.png)

Tracks:
- Edge stability over time
- Degradation detection

---

## 📊 Rolling Win Rate

![Rolling Winrate](images/rolling_winrate.png)

Used for:
- Stability validation
- Regime sensitivity detection

---

## 📡 Market Regime Radar

![Market Regime](images/market_regime.png)

Contextual visualization of:
- Trend intensity
- Volatility state
- Momentum alignment

---

## 📉 Price Regression

![Price Regression](images/price_regression.png)

Evaluates:
- Structural bias
- Slope persistence
- Mean reversion behavior

---

## 📊 Association Rule Mining (FP-Growth)

![Rule Strength](images/rule_strength.png)

FP-Growth is used to:

- Discover recurring winning combinations
- Quantify rule strength (Support, Confidence, Lift)
- Improve deterministic metric design

This supports strategy refinement through pattern validation.

---

# 8️⃣ Reliability & Production Considerations

- Idempotent JDBC writes
- Left-anti join duplication prevention
- Config-driven metric activation
- Airflow retry & scheduling control
- Partition-aware Spark execution
- Symbol-isolated processing

The system is designed to be:

- Re-runnable
- Recoverable
- Extensible
- Debuggable

---

# 9️⃣ Technical Stack

| Layer | Technology |
|-------|------------|
| Streaming | Kafka |
| Processing | Apache Spark |
| Orchestration | Airflow |
| Storage | MySQL |
| API | Flask |
| ML Pattern Mining | Spark ML (FP-Growth) |
| Visualization | Custom Dashboard |

---

# 🔟 Skills & Value Gained

## Financial Domain
- Market microstructure understanding
- Momentum & volatility regimes
- Risk management design
- Edge quantification

## Data Engineering
- Distributed processing (Spark)
- Streaming architecture (Kafka)
- Workflow orchestration (Airflow)
- Idempotent pipeline design
- Data warehouse modeling

## Data Analytics & ML
- Feature engineering
- Deterministic scoring systems
- Backtest validation
- Expectancy modeling
- Association rule mining
- Regression analysis

## System Design
- Scalable architecture
- Failure recovery design
- Production-grade scheduling
- Observability mindset

---

# 🏁 Conclusion

This project is not just a crypto prediction tool.

It is a **production-oriented quantitative research infrastructure** demonstrating:

- Real-time streaming ingestion
- Distributed computation
- Structured data warehousing
- Deterministic trading signal modeling
- Statistical validation
- Pattern mining integration
- End-user analytics delivery

It reflects a complete data lifecycle —  
from raw event ingestion to actionable quantitative insight.
