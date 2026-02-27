-- =====================================================
-- DIMENSION: SYMBOL
-- Lưu danh sách các cặp giao dịch (dữ liệu tĩnh)
-- Dùng để join vào các fact table thay vì hardcode text
-- =====================================================
CREATE TABLE dim_symbol (
    symbol_id INT AUTO_INCREMENT PRIMARY KEY,   -- Surrogate key
    symbol_name VARCHAR(20) NOT NULL UNIQUE     -- Tên cặp (BTCUSDT, ETHUSDT...)
);

-- Seed dữ liệu cố định
INSERT INTO dim_symbol (symbol_name)
VALUES 
('BTCUSDT'), ('ETHUSDT'), ('BNBUSDT'), 
('PEPEUSDT'), ('XRPUSDT'), ('SOLUSDT'), 
('DOGEUSDT'), ('LINKUSDT');


-- =====================================================
-- DIMENSION: INTERVAL / TIMEFRAME
-- Lưu các khung thời gian của nến
-- =====================================================
CREATE TABLE dim_interval (
    interval_id INT AUTO_INCREMENT PRIMARY KEY,   -- Surrogate key
    interval_name VARCHAR(10) NOT NULL UNIQUE     -- 15m, 1h, 4h, 1d...
);

-- Seed dữ liệu timeframe
INSERT INTO dim_interval (interval_name)
VALUES
('15m'), ('30m'), ('1h'), ('2h'), ('4h'), ('1d');



-- =====================================================
-- FACT: KLINE (CANDLE DATA - REALTIME / CURRENT)
-- Grain: 1 symbol + 1 interval + 1 open_time
-- Đây là bảng time-series chính dùng cho:
-- - indicator calculation
-- - backtest
-- - model inference
-- - dashboard realtime
-- =====================================================
CREATE TABLE fact_kline (
    kline_id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- Surrogate key

    symbol_id INT NOT NULL,                       -- FK → dim_symbol
    interval_id INT NOT NULL,                     -- FK → dim_interval

    open_price DECIMAL(20,10) NOT NULL,           -- Giá mở cửa
    high_price DECIMAL(20,10) NOT NULL,           -- Giá cao nhất
    low_price DECIMAL(20,10) NOT NULL,            -- Giá thấp nhất
    close_price DECIMAL(20,10) NOT NULL,          -- Giá đóng cửa

    volume DECIMAL(38,18) NOT NULL,               -- Khối lượng giao dịch

    open_time DATETIME NOT NULL,                  -- Thời gian mở nến
    close_time DATETIME NOT NULL,                 -- Thời gian đóng nến

    -- ================= BUSINESS KEY =================
    -- Đảm bảo mỗi candle là duy nhất
    CONSTRAINT unique_kline 
        UNIQUE (symbol_id, interval_id, open_time),

    -- ================= PERFORMANCE INDEX =================
    -- Tối ưu:
    -- - join với prediction theo close_time
    -- - query time-series mới nhất
    INDEX idx_kline_symbol_interval_close_time 
        (symbol_id, interval_id, close_time),

    -- ================= FOREIGN KEY =================
    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES dim_interval(interval_id)
);


-- =====================================================
-- FACT: KLINE HISTORY (LƯU TRỮ DỮ LIỆU LỊCH SỬ)
-- Dùng cho:
-- - lưu trữ lâu dài
-- - giảm tải bảng realtime
-- - backtest full history
-- Cấu trúc giống hệt fact_kline
-- =====================================================
CREATE TABLE fact_kline_history LIKE fact_kline;