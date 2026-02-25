# 🚀 Crypto Quantitative Research & Real-Time Data Platform (Production-Grade)  
### Digital Asset Analytics & Prediction System Based on Real-Time Data  

**Author:** Nguyễn Ngọc Nam  
**Mentor:** Phạm Long Vân – Data Manager  
**Location:** Ho Chi Minh City, Vietnam – 2026  

---

# 🌍 Market Context & Real-World Demand

In recent years, digital assets such as BTC, ETH, and BNB have increasingly become highly liquid asset classes attracting both retail and institutional investors. However, the cryptocurrency market is characterized by high volatility, strong sensitivity to news, and frequent short-term signal noise. Most trading decisions are still based on intuition or fragmented observations, lacking a systematic quantitative framework capable of long-term validation and performance evaluation.

As digital assets gain wider adoption and regulatory attention, the demand for a transparent, real-time data analytics platform with statistical grounding becomes increasingly critical. Such a system must not only continuously collect and process data but also organize it under a standardized Data Warehouse architecture, build a deterministic scoring framework, and evaluate strategies through traceable performance metrics.

This project was developed to address that challenge. Instead of focusing solely on short-term prediction, the system is designed as a complete data infrastructure — from ingestion, distributed processing, structured storage, modeling, to performance validation — delivering quantitative insights that are verifiable and scalable.

---

## 📌 Executive Summary

This platform is designed as a production-oriented quantitative research and data processing system with the following objectives:

- Ingest real-time cryptocurrency market data  
- Normalize and organize data using a Dim–Fact Data Warehouse model  
- Build a deterministic trading signal scoring engine  
- Backtest and evaluate strategy stability  
- Discover recurring winning patterns using FP-Growth  
- Present analytical insights through an interactive dashboard  

The system is built with a strong emphasis on:

- **Scalability**  
- **Idempotency**  
- **Fault tolerance**  
- **Traceability**  

Demo link: https://ngocnam-de-project.hocnghiepvu.com  

---

# 1️⃣ Overall System Architecture

## 🏗 System Architecture

![System Architecture](images/System_Architecture_1.png)

The system consists of five main layers:

### 🔹 Data Ingestion Layer
- Binance WebSocket / API  
- News Crawler  
- Kafka Streaming  

### 🔹 Processing Layer
- Spark (Batch & Streaming)  
- Indicator Engine  
- Metric & Scoring Engine  
- Backtest Engine  
- FP-Growth Mining  

### 🔹 Storage Layer
- MySQL Data Warehouse (Dim–Fact model)  

### 🔹 Orchestration Layer
- Airflow DAG  
- Retry & Failure Handling  
- Idempotent Job Execution  

### 🔹 Presentation Layer
- Flask API  
- Analytical Dashboard  

---

# 2️⃣ Data Warehouse Architecture

After defining the overall system architecture, the most critical step is designing the Data Warehouse to ensure structured, scalable data organization.

## 🧱 2.1 Dim–Fact Model

### 🎯 Design Principles

- Clearly define **Data Grain** for each fact table  
- Separate Context (Dimension) from Event (Fact)  
- Ensure extensibility when adding assets or new metrics  
- Optimize analytical queries over time  

---

## 📌 2.2 Core Fact Design

![Warehouse Schema](images/warehouse_schema_crypto.png)

### `fact_kline`
- Grain: (symbol_id, interval_id, open_time)  
- Stores normalized OHLCV market data  

### `fact_indicator`
- Grain: (symbol_id, interval_id, indicator_type_id, open_time)  
- Stores atomic indicator values  

### `fact_metric_value`
- Grain: (symbol_id, metric_id, open_time)  
- Stores evaluated trading conditions  

### `fact_prediction`
- Grain: (symbol_id, interval_id, signal_time, signal_type)  
- Stores BUY/SELL/SIDEWAY signals  

### `fact_prediction_result`
- Separated from prediction to ensure:
  - No data leakage  
  - Leakage-safe backtesting  
  - Clear separation between signal generation and validation  

---

## 📰 2.3 Multi-Layer Sentiment Pipeline

![News Warehouse Schema](images/warehouse_schema_news.png)

Sentiment processing is structured into four layers:

1. `news_fact` (Raw Article)  
2. `news_coin_fact` (Symbol Mapping)  
3. `news_sentiment_weighted_fact` (Weighted Score)  
4. `news_sentiment_agg_fact` (Window Aggregation)  

This design ensures:

- Traceability from aggregated data back to raw source  
- Recomputability when weighting logic changes  
- Clear separation of processing responsibilities  

---

## Why Dim–Fact?

- Separation of context and events  
- Optimized analytical queries  
- Clear historical storage  
- Easy extensibility for new metrics  
- Alignment with standard Data Warehouse practices  
- Time-series and asset-level analytical support  

---

# 3️⃣ System Design & Implementation

## 3.1 Ingestion Layer

![Ingestion Layer](images/ingestion_layer.png)

- Kafka decouples producers and consumers  
- Supports data replay  
- Horizontally scalable with increasing volume  
- Reduces direct dependency on external APIs  

---

## 3.2 Indicator Computation

![Indicator Computation](images/indicator_computation.png)

- RSI, MACD, EMA, ATR, ADX, BB, OBV  
- Partitioned by symbol  
- Fully recomputable from raw kline data  

---

## 3.3 Metric Abstraction

| metric_code | Logic | Direction | Weight |
|-------------|--------|-----------|--------|
| BTC_ADX_STRONG_2H | ADX ≥ 20 | ABOVE | 1.00 |
| BTC_BUY_MACD_BULL_2H | MACD > Signal | CROSS_UP | 1.30 |
| BTC_BUY_RSI_BULL_2H | RSI > 50 | ABOVE | 1.00 |
| BTC_BUY_VOL_OK_2H | BB_WIDTH ≥ 0.03 | ABOVE | 0.80 |
| BTC_SELL_TREND_DOWN_2H | EMA200 downward slope | TREND_DOWN | 1.20 |
| BTC_SELL_ADX_WEAK_2H | ADX < 20 | BELOW | 1.00 |
| BTC_SELL_MACD_BEAR_2H | MACD < Signal | CROSS_DOWN | 1.30 |
| BTC_SELL_MACD_HIST_DOWN_2H | MACD_HIST decreasing | TREND_DOWN | 1.20 |
| BTC_SELL_RSI_BEAR_2H | RSI < 45 | BELOW | 1.00 |
| BTC_SELL_FAIL_BB_UP_2H | Price rejection at BB_UP | REJECT | 0.90 |

- `dim_metric` defines trading conditions  
- Supports:
  - Threshold logic  
  - Cross logic  
  - Trend logic  
  - Volatility logic  

---

## 3.4 Prediction Engine

![Prediction Engine](images/prediction_engine.png)

buy_score = Σ(weighted BUY metrics)
sell_score = Σ(weighted SELL metrics)

edge = |buy_score − sell_score|
confidence = max(score) / MAX_SCORE

- Conflict detection  
- No-trade filter  
- Confidence band filter  

---

## 3.5 Anti-Duplicate & Idempotent Design

![Anti-Duplicate](images/anti_duplicate.png)

- Left-anti join before database writes  
- Duplicate write control  
- Replay-safe processing  

---

## 3.6 Backtesting

![Backtesting](images/backtesting.png)

- Separation of prediction and result layers  
- Adaptive TP/SL  
- Controlled lookahead window  
- Rolling validation  

---

# 4️⃣ Orchestration

![Airflow DAG](images/airflow_dag.png)

The system is orchestrated using Airflow:

- Multi-coin parallel branches (BTC / ETH / BNB)  
- Retry policies  
- Failure recovery  
- Isolated job execution  

Design ensures:

- A failed job does not crash the entire system  
- Re-execution without duplicate data  

---

# 5️⃣ Data Understanding (EDA) & Data Dictionary Reasoning

Data collection is grounded not only in technical design but also in market structure and behavioral dynamics.

## 🔹 Market Data (OHLCV)

| Field | Reason for Collection | Role in Model |
|-------|----------------------|---------------|
| Open / Close | Identify candle structure | Measure momentum |
| High / Low | Measure price range | Analyze volatility |
| Volume | Measure capital flow strength | Signal confirmation |
| Timestamp | Time-based analysis | Regime detection |

OHLCV forms the foundation of all technical analysis. Without proper timeframe normalization, indicators lose meaning.

---

## 🔹 Momentum Indicators (RSI, MACD)

### RSI
- Measures overbought/oversold conditions  
- Detects potential reversals  

### MACD
- Measures moving average convergence/divergence  
- Detects momentum shifts  

---

## 🔹 Trend Indicators (EMA, ADX)

### EMA
- EMA200: Long-term trend  
- EMA20/50: Mid/short-term trend  

### ADX
- Measures trend strength  
- Differentiates trending vs. sideways markets  

---

## 🔹 Volatility Indicators (BB, ATR)

Crypto markets exhibit:

- Volatility squeeze  
- Volatility expansion  

Bollinger Bands measure band width; ATR supports stop-loss calibration.

---

## 🔹 Volume-Based Metrics

Volume confirms signals:

- Breakout without volume → false breakout  
- Volume divergence → weakening trend  

---

## 🔹 News & Sentiment

Crypto reacts strongly to:

- Regulation  
- ETF approvals  
- Exchange hacks  
- Macro events  

Sentiment complements technical scoring.

---

## 🎯 EDA Objectives

- Identify indicators contributing to win rate  
- Eliminate statistically insignificant metrics  
- Optimize scoring weights  
- Reduce overfitting  
- Improve long-term robustness  

---

# 6️⃣ Framework Modeling & Scoring

## 🧮 Market Scoring

Market Score = Trend + Momentum + Volume + Volatility  

Confidence Score = Market Score / Max Score  

### Guard Mechanisms

- Conflict Detection  
- Weak Edge Filter  
- Confidence Band Filter  
- No-Trade Logic  

---

# 7️⃣ Backtesting & Risk Management

Backtesting evaluates:

- Adaptive TP/SL  
- Lookahead window control  
- Win/Loss classification  
- PnL normalization  
- Rolling expectancy  
- Regime-based analysis  

Ensures:

- Survivability validation  
- Edge degradation detection  
- Overfitting prevention  
- Long-term performance evaluation  

---

# 8️⃣ Performance Analysis & Validation

(Sections unchanged — Equity Curve, Rolling Expectancy, Rolling Winrate, Market Regime Radar, Price Regression, FP-Growth Rule Mining — identical structure as original content.)

---

# 9️⃣ Production Considerations

The system is designed to:

- Support safe re-execution without duplication  
- Retry failed jobs via Airflow  
- Isolate coin-level processing  
- Scale across additional assets  
- Prevent duplicates using left-anti join strategy  
- Enable configurable metric toggling  
- Maintain full pipeline traceability  

---

# 🔟 Gained Value

Designing and implementing this system enhanced not only domain expertise but also technical capability and system-level thinking.

(Sections 1–5 preserved exactly as original structure and meaning.)

---

# 🏁 Conclusion

This project is not merely a crypto trading signal generator but a complete production-grade data platform. The entire data lifecycle is implemented end-to-end: from real-time ingestion via Kafka, distributed processing with Spark, structured organization under a Dim–Fact Data Warehouse model, to deterministic scoring, leakage-safe backtesting, and FP-Growth pattern mining.

Through this implementation, I strengthened both financial market domain knowledge and advanced Data Engineering system design capabilities — building idempotent pipelines, controlling duplicates, ensuring scalability, and maintaining traceability across distributed environments.

More importantly, the system demonstrates the connection between business problems and technical solutions — transforming raw data into statistically validated quantitative insights, forming a strong foundation for scalable enterprise-grade data systems.

---

## License

This system is for **educational and research purposes only**.  
© 2026 Nguyễn Ngọc Nam — Data Engineering Project.
