# 🚀 Deterministic Quantitative Trading Research Platform  
### Hệ Thống Phân Tích & Dự Đoán Tài Sản Số Dựa Trên Dữ Liệu Thời Gian Thực  

**Author:** Nguyễn Ngọc Nam  
**Mentor:** Phạm Long Vân - Data Manager  
**Location:** Ho Chi Minh City, Vietnam — 2025  

---

# 1️⃣ Bối Cảnh Thực Tiễn & Động Lực Xây Dựng

Tài sản số (crypto assets) như BTC, ETH và BNB đang dần trở thành một lớp tài sản có ảnh hưởng thực sự trong hệ sinh thái tài chính toàn cầu. Không chỉ nhà đầu tư cá nhân, mà cả tổ chức và chính phủ cũng bắt đầu quan tâm đến việc quản lý và cấp phép loại tài sản này. Khi khung pháp lý dần hình thành, nhu cầu về một hệ thống phân tích dữ liệu đáng tin cậy và có cơ chế kiểm chứng trở nên cấp thiết.

Trong thực tế, phần lớn nhà đầu tư cá nhân hiện nay vẫn ra quyết định dựa trên cảm tính, tin tức rời rạc hoặc các công cụ phân tích thiếu kiểm định hiệu suất. Họ thiếu một môi trường có thể:

- Thu thập dữ liệu liên tục  
- Phân tích có cấu trúc  
- Kiểm định tín hiệu trước khi sử dụng  
- Đánh giá độ ổn định chiến lược theo thời gian  

Dự án này được xây dựng xuất phát từ nhu cầu đó: cung cấp cho người dùng một hệ thống phân tích định lượng có khả năng xử lý dữ liệu liên tục, tích hợp cả yếu tố kỹ thuật lẫn tâm lý thị trường, đồng thời có cơ chế kiểm chứng rõ ràng.

Hệ thống tập trung vào BTC, ETH và BNB vì đây là các tài sản có:

- Thanh khoản cao  
- Lịch sử dữ liệu dài  
- Độ ổn định tương đối  
- Ảnh hưởng lớn đến thị trường  

Việc chọn nhóm tài sản này giúp đảm bảo tính thực tiễn và tính bền vững của phân tích.

---

# 2️⃣ Xác Định Bài Toán & Hướng Giải Quyết

Bài toán đặt ra không đơn thuần là sinh tín hiệu BUY/SELL, mà là xây dựng một hệ thống hoàn chỉnh có khả năng:

- Thu thập dữ liệu real-time  
- Chuẩn hóa và lưu trữ có cấu trúc  
- Phân tích kỹ thuật & sentiment  
- Sinh tín hiệu có thể giải thích  
- Kiểm chứng hiệu suất  
- Trình diễn cho end-user  

---

# 3️⃣ Thu Thập Dữ Liệu & Phân Tích EDA

## 3.1 Dữ Liệu Giá (OHLCV)

Dữ liệu được stream real-time từ Binance thông qua API/WebSocket.

Các trường chính:

- Open  
- High  
- Low  
- Close  
- Volume  

Close phản ánh điểm đồng thuận cuối cùng của thị trường.  
High và Low cho biết mức độ biến động.  
Volume phản ánh dòng tiền.

Các chỉ báo kỹ thuật như RSI, EMA, MACD đều được xây dựng từ cấu trúc này.

## 3.2 Dữ Liệu Tin Tức & Sentiment

Tin tức được crawl từ các trang crypto và xử lý sentiment.

Lý do thu thập sentiment:

- Crypto phản ứng mạnh với tin tức  
- Tâm lý ảnh hưởng lớn đến biến động  
- Indicator kỹ thuật không giải thích hết biến động  

Việc kết hợp kỹ thuật và sentiment giúp tăng độ toàn diện của phân tích.

---

# 4️⃣ Thiết Kế Kiến Trúc Hệ Thống

![System Architecture](images/System_Architecture.png)

Luồng tổng thể:

Market / News → Kafka → Spark → Data Warehouse → Metric Engine → Prediction → Backtesting → Analytics → Web

## 4.1 Vì Sao Sử Dụng Kafka?

- Xử lý streaming 24/7  
- Tách ingestion khỏi processing  
- Cho phép replay dữ liệu  
- Tăng fault tolerance  

## 4.2 Vì Sao Sử Dụng Spark?

- Xử lý rolling window  
- Tính toán indicator phân tán  
- Xử lý sentiment batch lớn  

## 🗄️ Data Warehouse Schema

![Warehouse Schema](images/warehouse_schema.png)

Thiết kế Dim–Fact:

Dimension:
- dim_symbol  
- dim_interval  
- dim_indicator_type  
- dim_metric  

Fact:
- fact_kline  
- fact_indicator  
- fact_metric_value  
- fact_prediction  
- fact_prediction_result

## 4.3 Vì Sao Thiết Kế Theo Dim–Fact?

Dimension chứa thông tin mô tả.  
Fact chứa sự kiện theo thời gian.

Thiết kế này:

- Chuẩn hóa dữ liệu  
- Hỗ trợ truy vết  
- Tránh redundancy  
- Dễ audit  
- Phù hợp chuẩn Data Warehouse  

Nguyên tắc:
- Explicit grain  
- Idempotent ETL  
- Không overwrite lịch sử  
- Truy vết vòng đời tín hiệu

---

# 5️⃣ Orchestration & Reliability

Apache Airflow:

- Lập lịch  
- Retry  
- Dependency control  
- Monitoring  

Pipeline được thiết kế idempotent để tránh duplicate.

---

# 6️⃣ Indicator, Metric & Prediction Engine

Indicator → Metric logic → Weighted scoring → Prediction

Hệ thống deterministic để:

- Có thể giải thích  
- Có thể audit  
- Không black-box  

---

# 7️⃣ Vì Sao Sử Dụng FP-Growth?

FP-Growth được dùng để:

- Phát hiện pattern trong trade thắng  
- Kiểm tra lift & confidence  
- Xác nhận cấu trúc chiến lược  

Không dùng trực tiếp để dự đoán, mà để kiểm chứng tính bền vững.

---

# 8️⃣ Analytics & Dashboard

Hệ thống không chỉ sinh tín hiệu giao dịch mà còn cung cấp bộ dashboard phân tích nhằm giúp người dùng đánh giá độ bền vững và rủi ro của chiến lược.

---

## 📈 1. Price Regression Analysis

![Price Regression](images/price_regression.png)

Biểu đồ hồi quy tuyến tính dùng để đo độ dốc xu hướng ngắn hạn.

- Slope dương → xu hướng tăng  
- Slope âm → xu hướng giảm  
- R² thấp → xu hướng yếu, nhiễu cao  

Regression đóng vai trò context filter cho prediction engine.

---

## 📊 2. Model Stability – Rolling Win-rate

![Rolling Winrate](images/rolling_winrate.png)

Đo win-rate theo cửa sổ trượt.

Mục đích:

- Phát hiện regime shift  
- Kiểm tra độ ổn định theo thời gian  
- Tránh overfitting  

---

## 🧩 3. Rule Strength – FP-Growth

![Rule Strength](images/rule_strength.png)

Hiển thị các tổ hợp metric thường xuất hiện trong trade thắng.

- Xác định feature co-occurrence  
- Kiểm tra support & lift  
- Phát hiện cấu trúc tạo edge  

---

## 🧭 4. Market Regime Impact

![Market Regime](images/market_regime.png)

Radar chart thể hiện trạng thái thị trường:

- Trend  
- Volatility  
- Momentum  
- Edge Strength  

Giúp hiểu bối cảnh thị trường hiện tại.

---

## 📉 5. Rolling Expectancy Curve

![Rolling Expectancy](images/rolling_expectancy.png)

Expectancy = (Win% × Avg Win) − (Loss% × Avg Loss)

- Expectancy > 0 → chiến lược có edge  
- Expectancy < 0 → không có lợi thế  

---

## 💰 6. Equity Curve & Drawdown

![Equity Curve](images/equity_curve.png)

Equity Curve mô phỏng tăng trưởng vốn.  
Drawdown phản ánh rủi ro thực tế.

Đây là chỉ số quan trọng nhất với end-user.

---

## 🎯 Vì Sao Dashboard Phù Hợp Cho End-User?

Dashboard được thiết kế để trả lời các câu hỏi thực tế mà nhà đầu tư quan tâm: chiến lược có tạo ra lợi nhuận bền vững không, mức drawdown có chấp nhận được không, edge có duy trì theo thời gian không và thị trường hiện tại có phù hợp để giao dịch hay không.

Thay vì chỉ hiển thị chỉ báo kỹ thuật hoặc độ chính xác mô hình, hệ thống tập trung vào equity, drawdown, expectancy và stability — những yếu tố quyết định khả năng áp dụng ngoài đời thực. Vì vậy dashboard không chỉ phục vụ research mà còn có thể dùng để hỗ trợ quyết định đầu tư thực tế.

---

# 🔟 Kết Luận & Giá Trị Đạt Được

Dự án này không chỉ xây dựng một pipeline crypto, mà là quá trình thiết kế một hệ thống định lượng dựa trên nhu cầu thực tế của thị trường tài sản số.

Quá trình thực hiện giúp nâng cao hiểu biết về domain tài chính, quản trị rủi ro, Data Engineering (Kafka, Spark, Airflow), thiết kế Data Warehouse và tư duy kiến trúc hệ thống.

Quan trọng hơn, dự án hình thành tư duy thiết kế có kiểm soát, có kiểm chứng và có khả năng tạo giá trị cho end-user. Thay vì xử lý dữ liệu rời rạc, hệ thống được xây dựng như một nền tảng có thể mở rộng, audit và cải tiến liên tục.

Đây là bước chuyển từ tư duy lập trình sang tư duy kiến trúc phục vụ thực tiễn.
