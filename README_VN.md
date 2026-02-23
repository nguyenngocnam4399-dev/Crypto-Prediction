# 🚀 Deterministic Quantitative Crypto Research Platform  
### Real-Time Crypto Prediction & Analytics System (BTC • ETH • BNB)

---

# 1️⃣ Problem Statement – Nhu Cầu Thực Tế

Trong những năm gần đây, tài sản số (Digital Assets) đã trở thành một phần quan trọng của thị trường tài chính toàn cầu.

- Số lượng nhà đầu tư quan tâm đến BTC, ETH, BNB ngày càng lớn  
- Thanh khoản thị trường cao  
- Biến động mạnh nhưng có tính chu kỳ  
- Nhà nước đang dần tiến tới quản lý và cấp phép tài sản số  

Tuy nhiên, phần lớn nhà đầu tư vẫn:

- Ra quyết định dựa trên cảm tính  
- Không có hệ thống định lượng  
- Không đánh giá được tính ổn định dài hạn của chiến lược  

---

## 🎯 Mục tiêu dự án

Xây dựng một hệ thống:

- Thu thập dữ liệu crypto real-time
- Chuẩn hóa và lưu trữ theo mô hình Data Warehouse
- Ứng dụng mô hình Statistical & Machine Learning
- Đánh giá hiệu suất bằng backtest
- Phân tích độ ổn định chiến lược
- Trình diễn trực quan cho end-user

Hệ thống hướng tới:

> Cung cấp góc nhìn định lượng có độ chính xác tương đối tốt  
> Có thể áp dụng cho cá nhân và mở rộng cho end-user.

---

# 2️⃣ Solution Overview – Giải Quyết Bài Toán

Hệ thống được xây dựng theo flow:

1. Thu thập dữ liệu real-time từ Binance (WebSocket / API)  
2. Streaming qua Kafka  
3. Spark xử lý & tính toán indicator  
4. Lưu trữ vào Data Warehouse (Dim-Fact)  
5. Xây dựng Metric & Scoring  
6. Prediction Engine  
7. Backtest & Confirmation  
8. FP-Growth Pattern Mining  
9. Flask API + Dashboard Visualization  

---

# 🏗 System Architecture

![System Architecture](images/System_Architecture.png)

---

# 3️⃣ Data Collection & Data Dictionary

## 📊 Market Data (OHLCV)

Thu thập:

- Open  
- High  
- Low  
- Close  
- Volume  
- Timestamp  

### Vì sao cần?

| Dữ liệu | Ý nghĩa tài chính |
|----------|------------------|
| Open/Close | Giá đóng mở kỳ |
| High/Low | Biên độ biến động |
| Volume | Sức mạnh thị trường |
| Time | Chu kỳ & trend |

---

## 📰 News Sentiment

- Thu thập tin tức crypto  
- Tính sentiment score  
- Weight theo tag & độ tin cậy  

### Vì sao cần?

- Crypto phản ứng mạnh với tin tức  
- Sentiment ảnh hưởng ngắn hạn tới price  

---

# 🗄 Data Warehouse Design

![Warehouse Schema](images/warehouse_schema_crypto.png)

![News Warehouse Schema](images/warehouse_schema_news.png)

---

## 🎯 Vì sao thiết kế Dim-Fact?

- Chuẩn hóa dữ liệu  
- Truy vết lịch sử  
- Tối ưu query  
- Idempotent write  
- Phù hợp DW concept  

---

# 4️⃣ Data Engineering Layer

## 🔄 Kafka Streaming

- Real-time ingestion  
- Decoupled producer & consumer  

## ⚡ Spark Processing

- Indicator calculation  
- Metric scoring  
- Prediction logic  
- Backtest confirmation  

## 🕒 Airflow Scheduling

- Batch job orchestration  
- Retry logic  
- Monitoring  
- Idempotent pipeline  

---

# 5️⃣ Modeling & Statistical Design

## 📈 Indicator Layer

- RSI  
- MACD  
- EMA  
- Bollinger Bands  
- ADX  
- VWAP  
- ATR  
- OBV  

---

## 🧮 Metric & Scoring Engine

Market Score =  
Trend + Momentum + Volume + Volatility  

Confidence Score =  
Market Score / Max Score  

No-Trade Guard:

- Conflict detection  
- Weak edge filter  
- Over-confidence filter  

---

## 🔁 Backtest & Confirmation

- Take Profit / Stop Loss  
- Lookahead window  
- Win / Loss classification  
- PnL calculation  
- Survivability metric  

---

# 6️⃣ Performance Analytics

## 📊 Equity Curve & Drawdown

![Equity Curve](images/equity_curve.png)

- Đánh giá tăng trưởng vốn  
- Đo độ sụt giảm tối đa  
- Kiểm tra survivability  

---

## 📉 Rolling Expectancy

![Rolling Expectancy](images/rolling_expectancy.png)

- Kiểm tra edge theo thời gian  
- Nếu expectancy < 0 → mất lợi thế  

---

## 📈 Rolling Win-rate

![Rolling Winrate](images/rolling_winrate.png)

- Độ ổn định theo thời gian  
- Kiểm tra variance  

---

## 📡 Market Regime Radar

![Market Regime](images/market_regime.png)

- Context filter  
- Không phải tín hiệu trực tiếp  

---

## 📉 Price Regression Analysis

![Price Regression](images/price_regression.png)

- Phân tích độ dốc  
- Kiểm tra bias xu hướng  

---

## 📊 Rule Strength (FP-Growth)

![Rule Strength](images/rule_strength.png)

---

# 7️⃣ FP-Growth Pattern Mining

### Vì sao dùng FP-Growth?

- Tìm pattern WIN trades  
- Phân tích feature co-occurrence  
- Không dự đoán trực tiếp  
- Hỗ trợ strategy refinement  

### Metrics:

- Support  
- Confidence  
- Lift  

---

# 8️⃣ End-User Value

Hệ thống giúp end-user:

- Nhìn thấy prediction rõ ràng  
- Hiểu mức độ confidence  
- Đánh giá risk  
- Kiểm tra stability  
- Không phụ thuộc cảm tính  

---

# 9️⃣ Technical Highlights

- Kafka real-time streaming  
- Spark distributed processing  
- Airflow orchestration  
- MySQL Data Warehouse  
- Dim-Fact modeling  
- Idempotent pipeline  
- Anti-duplicate insert  
- Conflict detection logic  
- Dual TP/SL dynamic  
- FP-Growth ML integration  
- Flask Reporting API  

---

# 🔟 What I Gained (Gen Value)

## 📊 Financial Domain

- Hiểu OHLC structure  
- Momentum & volatility  
- Risk management  
- Take Profit / Stop Loss  
- Edge concept  

## 🏗 Data Engineering

- Kafka  
- Spark  
- Airflow  
- JDBC optimization  
- Monitoring & scheduling  

## 🗄 Data Warehouse

- Dim-Fact modeling  
- Star schema  
- Query optimization  
- Historical tracking  

## 📈 Data Analytics & Data Science

- Metric design  
- Scoring system  
- Backtesting methodology  
- Expectancy calculation  
- FP-Growth modeling  
- Regression analysis  

## 🎨 UI / UX

- Dashboard design  
- Performance communication  
- Visualization clarity  

---

# 🔚 Conclusion

Dự án này không chỉ là một hệ thống dự đoán crypto.

Nó là một nền tảng:

- Data Engineering chuẩn production  
- Data Warehouse đúng concept  
- Statistical modeling ứng dụng thực tế  
- Analytics đầy đủ survivability & stability  
- Có thể mở rộng cho end-user  
