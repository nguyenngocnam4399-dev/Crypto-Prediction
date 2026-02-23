# 🚀 Crypto Quantitative Research & Real-Time Data Platform (Production-Grade) 
### Hệ Thống Phân Tích & Dự Đoán Tài Sản Số Dựa Trên Dữ Liệu Thời Gian Thực  

**Author:** Nguyễn Ngọc Nam  
**Mentor:** Phạm Long Vân - Data Manager  
**Location:** Ho Chi Minh City, Vietnam - 2026

---

# 🌍 Bối Cảnh Thị Trường & Nhu Cầu Thực Tế

Trong những năm gần đây, tài sản số như BTC, ETH và BNB đang dần trở thành một lớp tài sản có thanh khoản cao và thu hút sự tham gia mạnh mẽ của nhà đầu tư cá nhân lẫn tổ chức. Tuy nhiên, đặc thù của thị trường crypto là biến động lớn, phản ứng nhạy với tin tức và thường xuyên xuất hiện nhiễu tín hiệu ngắn hạn. Phần lớn quyết định giao dịch vẫn dựa trên cảm tính hoặc quan sát rời rạc, thiếu một hệ thống định lượng có khả năng kiểm chứng và đánh giá hiệu suất dài hạn.

Trong bối cảnh tài sản số ngày càng được quan tâm và có xu hướng được quản lý chính thức, nhu cầu về một nền tảng phân tích dữ liệu thời gian thực, minh bạch và có cơ sở thống kê trở nên cấp thiết. Một hệ thống như vậy không chỉ cần thu thập và xử lý dữ liệu liên tục, mà còn phải tổ chức dữ liệu theo chuẩn Data Warehouse, xây dựng mô hình scoring rõ ràng, và đánh giá chiến lược bằng các chỉ số hiệu suất có thể truy vết.

Dự án này được xây dựng nhằm giải quyết bài toán đó. Thay vì tập trung vào dự đoán ngắn hạn đơn lẻ, hệ thống hướng tới việc thiết kế một hạ tầng dữ liệu hoàn chỉnh — từ ingestion, xử lý phân tán, lưu trữ chuẩn hóa, modeling, đến phân tích hiệu suất — nhằm cung cấp góc nhìn định lượng có thể kiểm chứng và mở rộng.

---

## 📌 Tóm Tắt Điều Hành (Executive Summary)

Đây là một nền tảng xử lý dữ liệu và nghiên cứu định lượng được thiết kế theo định hướng production, với mục tiêu:

- Thu thập dữ liệu thị trường crypto theo thời gian thực
- Chuẩn hóa và tổ chức dữ liệu theo mô hình Data Warehouse (Dim-Fact)
- Xây dựng hệ thống chấm điểm (scoring) tín hiệu giao dịch có tính xác định (deterministic)
- Backtest và đánh giá độ ổn định chiến lược
- Khai phá pattern giao dịch thắng bằng FP-Growth
- Trình diễn phân tích qua Dashboard

Hệ thống được xây dựng với tư duy:

- Scalability (khả năng mở rộng)
- Idempotency (chạy lại không trùng lặp)
- Fault tolerance (chịu lỗi)
- Traceability (truy vết dữ liệu)
  
Link demo: https://ngocnam-de-project.hocnghiepvu.com

---

# 1️⃣ Kiến Trúc Tổng Thể Hệ Thống

## 🏗 System Architecture

![System Architecture](images/System_Architecture.png)

Hệ thống gồm 5 tầng:

### 🔹 Tầng Thu Thập Dữ Liệu
- Binance WebSocket / API
- News Crawler
- Kafka Streaming

### 🔹 Tầng Xử Lý
- Spark (Batch & Streaming)
- Indicator Engine
- Metric & Scoring Engine
- Backtest Engine
- FP-Growth Mining

### 🔹 Tầng Lưu Trữ
- MySQL Data Warehouse (Dim-Fact)

### 🔹 Tầng Điều Phối
- Airflow DAG
- Retry & Failure Handling
- Idempotent Job Execution

### 🔹 Tầng Trình Diễn
- Flask API
- Dashboard phân tích

---

# 2️⃣ Thiết Kế Data Engineering

## 🔄 Ingestion Real-Time

- Kafka giúp tách biệt producer & consumer
- Hỗ trợ replay dữ liệu
- Có thể scale ngang khi volume tăng
- Giảm phụ thuộc trực tiếp vào nguồn API

## ⚡ Xử Lý Phân Tán (Spark)

Spark được dùng để:

- Tính indicator (RSI, MACD, EMA, BB, ADX, VWAP, ATR, OBV)
- Tính metric giao dịch
- Xây dựng market score
- Xác nhận backtest
- Chuẩn bị dữ liệu mining

Thiết kế đảm bảo:

- Partition theo symbol
- Anti-duplicate write (left-anti join)
- Idempotent JDBC insert
- Tách biệt xử lý theo từng tài sản

---

# 🔍 3️⃣ Data Pipeline Architecture Breakdown

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

# 📊 4️⃣ Data Understanding (EDA) & Data Dictionary Reasoning

Việc thu thập dữ liệu trong hệ thống không chỉ mang tính kỹ thuật mà dựa trên cơ chế hình thành giá và hành vi thị trường crypto.

## 🔹 Market Data (OHLCV)

| Trường dữ liệu | Lý do thu thập | Vai trò trong mô hình |
|---------------|---------------|------------------------|
| Open / Close  | Xác định cấu trúc nến | Đo động lượng |
| High / Low    | Đo biên độ dao động | Phân tích volatility |
| Volume        | Đo sức mạnh dòng tiền | Xác nhận tín hiệu |
| Timestamp     | Phân tích theo chu kỳ | Phân tích regime |

OHLCV là nền tảng của mọi phân tích kỹ thuật. Nếu không chuẩn hóa theo timeframe, mọi indicator sẽ mất ý nghĩa.

---

## 🔹 Momentum Indicators (RSI, MACD)

### RSI
- Đo trạng thái quá mua/quá bán
- Phát hiện khả năng đảo chiều

### MACD
- Đo sự hội tụ/phân kỳ trung bình động
- Phát hiện chuyển pha động lượng

Crypto thường xuất hiện pha biến động mạnh, do đó RSI & MACD rất phù hợp để đo momentum.

---

## 🔹 Trend Indicators (EMA, ADX)

### EMA
- EMA200: xu hướng dài hạn
- EMA20/50: xu hướng trung & ngắn hạn

### ADX
- Đo cường độ xu hướng
- Phân biệt trending và sideway

Giúp tránh giao dịch ngược xu hướng chính.

---

## 🔹 Volatility Indicators (BB, ATR)

Crypto có chu kỳ:
- Volatility squeeze
- Volatility expansion

BB đo độ mở biên  
ATR hỗ trợ thiết lập Stop Loss

---

## 🔹 Volume-Based Metrics

Volume xác nhận tín hiệu:

- Breakout không có volume → false breakout
- Divergence volume → dấu hiệu suy yếu

---

## 🔹 News & Sentiment

Crypto phản ứng mạnh với:
- Regulation
- ETF
- Exchange hack
- Macro event

Sentiment được sử dụng để bổ sung yếu tố tâm lý vào hệ thống scoring.

---

## 🎯 Mục tiêu EDA

- Xác định indicator đóng góp vào Win Rate
- Loại bỏ metric không có ý nghĩa thống kê
- Tối ưu trọng số scoring
- Giảm overfitting
- Cải thiện độ ổn định dài hạn

---

# 5️⃣ Kiến Trúc Data Warehouse

## 🗄 Mô Hình Dim-Fact

![Warehouse Schema](images/warehouse_schema_crypto.png)

![News Warehouse Schema](images/warehouse_schema_news.png)

### Dimension Tables
- dim_symbol
- dim_interval
- dim_indicator_type
- dim_metric
- tag_dim

### Fact Tables
- fact_kline
- fact_indicator
- fact_metric_value
- fact_prediction
- fact_prediction_result
- news_sentiment_weighted_fact
- fp_growth_win_patterns
- fp_growth_win_rules

---

## Vì Sao Chọn Dim-Fact?

- Tách biệt context và event
- Tối ưu truy vấn phân tích
- Lưu trữ lịch sử rõ ràng
- Dễ mở rộng metric mới
- Phù hợp chuẩn Data Warehouse
- Hỗ trợ phân tích theo thời gian và theo tài sản

---

# 6️⃣ Framework Modeling & Scoring

## 🧮 Market Scoring

Market Score =  Trend + Momentum + Volume + Volatility

Confidence Score =  Market Score / Max Score

### Cơ Chế Bảo Vệ (Guard)

- Conflict Detection
- Weak Edge Filter
- Confidence Band Filter
- No-Trade Logic

Mục tiêu:

- Tránh overtrading
- Hạn chế false signal
- Giảm nhiễu trong regime squeeze
- Duy trì tính ổn định chiến lược

---

# 7️⃣ Backtest & Quản Trị Rủi Ro

Backtest đánh giá:

- TP / SL động
- Lookahead window
- Win/Loss classification
- PnL normalization
- Rolling expectancy
- Phân tích theo regime

Đảm bảo:

- Kiểm tra tính sống sót (survivability)
- Phát hiện suy giảm edge
- Tránh overfitting
- Đánh giá hiệu suất dài hạn

---

# 8️⃣ Phân Tích Hiệu Suất

## 📈 Equity Curve & Drawdown

![Equity Curve](images/equity_curve.png)

- Tăng trưởng vốn
- Maximum drawdown
- Đánh giá rủi ro hệ thống

---

## 📉 Rolling Expectancy

![Rolling Expectancy](images/rolling_expectancy.png)

- Kiểm tra edge theo thời gian
- Phát hiện giai đoạn mất lợi thế

---

## 📊 Rolling Winrate

![Rolling Winrate](images/rolling_winrate.png)

- Đánh giá độ ổn định mô hình
- Phân tích độ nhạy regime

---

## 📡 Market Regime Radar

![Market Regime](images/market_regime.png)

Hiển thị:

- Trend strength
- Momentum alignment
- Volatility state

---

## 📉 Price Regression

![Price Regression](images/price_regression.png)

- Độ dốc xu hướng
- Bias thị trường
- Mean reversion behavior

---

## 📊 FP-Growth Rule Mining

![Rule Strength](images/rule_strength.png)

Sử dụng FP-Growth để:

- Tìm pattern giao dịch thắng lặp lại
- Đo strength bằng Support, Confidence, Lift
- Hỗ trợ tối ưu chiến lược
- Cải thiện hệ thống scoring

---

# 9️⃣ Yếu Tố Production

Hệ thống được thiết kế để:

- Có thể chạy lại không trùng dữ liệu
- Retry khi job lỗi qua Airflow
- Phân tách xử lý theo coin
- Hỗ trợ mở rộng thêm tài sản
- Kiểm soát duplicate bằng left-anti join
- Cấu hình metric bật/tắt linh hoạt
- Theo dõi và truy vết toàn bộ pipeline

---

# 🔟 Tech Stack

| Layer | Công nghệ |
|--------|------------|
| Streaming | Kafka |
| Processing | Apache Spark |
| Orchestration | Airflow |
| Storage | MySQL |
| API | Flask |
| ML Mining | Spark ML (FP-Growth) |
| Visualization | Custom Dashboard |

---

# 🔟 Giá Trị Đạt Được

## 📊 Kiến Thức Tài Chính
- Hiểu cấu trúc OHLC
- Momentum & Volatility
- Risk Management
- Edge Quantification

## 🏗 Data Engineering
- Spark Distributed Processing
- Kafka Streaming
- Airflow Orchestration
- Idempotent Pipeline Design
- Data Warehouse Modeling

## 📈 Data Analytics & ML
- Feature Engineering
- Deterministic Scoring
- Backtesting Methodology
- Expectancy Modeling
- Association Rule Mining
- Regression Analysis

## 🧠 System Design Mindset
- Thiết kế scalable
- Phục hồi khi lỗi
- Kiến trúc phân tầng rõ ràng
- Tư duy production-grade
- Kết nối giữa business problem và technical solution

---

# 🏁 Kết Luận

Dự án này không chỉ là một hệ thống tạo tín hiệu giao dịch crypto, mà là một nền tảng xử lý dữ liệu hoàn chỉnh được thiết kế theo tư duy production-grade. Toàn bộ vòng đời dữ liệu được triển khai xuyên suốt: từ ingestion thời gian thực qua Kafka, xử lý phân tán bằng Spark, tổ chức dữ liệu theo mô hình Dim-Fact trong Data Warehouse, đến xây dựng hệ thống scoring, backtest và khai phá pattern bằng FP-Growth.

Thông qua việc thiết kế và triển khai hệ thống này, tôi không chỉ củng cố kiến thức về cấu trúc thị trường tài chính (OHLC, momentum, volatility, risk management) mà còn nâng cao tư duy Data Engineering ở mức hệ thống: thiết kế pipeline có thể chạy lại không trùng lặp (idempotent), kiểm soát duplicate, đảm bảo khả năng mở rộng theo tài sản và theo khối lượng dữ liệu, cũng như duy trì tính truy vết và minh bạch trong phân tích.

Quan trọng hơn, dự án thể hiện cách kết nối giữa business problem và technical solution — chuyển đổi dữ liệu thô thành insight định lượng có khả năng kiểm chứng. Đây là nền tảng để phát triển các hệ thống phân tích dữ liệu ở quy mô lớn hơn, nơi độ ổn định, khả năng mở rộng và tính chính xác thống kê đóng vai trò cốt lõi.

---

## License
This system is for **educational and research purposes only**.  
© 2026 Nguyễn Ngọc Nam — Data Engineering Project.
