-- =====================================================
-- Dimension: Indicator Type
-- Lưu danh sách các loại indicator (dữ liệu tĩnh, dùng để join vào fact)
-- =====================================================
CREATE TABLE dim_indicator_type (
    type_id INT AUTO_INCREMENT PRIMARY KEY,     -- Surrogate key
    type_name VARCHAR(20) UNIQUE NOT NULL       -- Tên indicator (RSI14, EMA20...)
);

-- =====================================================
-- Seed dữ liệu cố định cho indicator
-- Phân nhóm theo logic trading để dễ maintain & filter dashboard
-- =====================================================
INSERT INTO dim_indicator_type (type_name)
VALUES
    -- Moving Average → xác định xu hướng
    ('SMA14'), ('EMA9'), ('EMA20'), ('EMA21'), ('EMA50'), ('EMA200'),

    -- Momentum → đo động lượng giá
    ('RSI14'), ('MACD_HIST'), ('STOCH_RSI'),

    -- Volatility → đo biến động
    ('ATR14'), ('BB_UP14'), ('BB_DOWN14'),

    -- Trend Strength → độ mạnh xu hướng
    ('ADX14'), ('PLUS_DI14'), ('MINUS_DI14'),

    -- Volume → dòng tiền
    ('OBV');


-- =====================================================
-- Fact: Indicator Value theo time-series
-- Grain: 1 indicator / 1 symbol / 1 interval / 1 timestamp
-- =====================================================
CREATE TABLE fact_indicator (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,  -- Surrogate key cho fact

    symbol_id INT NOT NULL,                -- FK → dim_symbol (BTCUSDT, ETHUSDT...)
    interval_id INT NOT NULL,              -- FK → dim_interval (1m, 5m, 1h...)
    type_id INT NOT NULL,                  -- FK → dim_indicator_type (RSI14, EMA20...)

    value DECIMAL(18,8),                   -- Giá trị indicator tại thời điểm đó

    timestamp DATETIME,                   -- Thời gian = close_time của kline
                                         -- Dùng để join với fact_kline & prediction

    -- ================= FK =================
    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES dim_interval(interval_id),
    FOREIGN KEY (type_id) REFERENCES dim_indicator_type(type_id),

    -- ================= Business Key =================
    -- Đảm bảo mỗi indicator tại 1 thời điểm chỉ có 1 giá trị duy nhất
    UNIQUE(symbol_id, interval_id, type_id, timestamp)
);