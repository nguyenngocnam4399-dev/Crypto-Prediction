-- =====================================================
-- DIMENSION: METRIC DEFINITION (SIGNAL CONFIGURATION)
-- Lưu metadata mô tả logic của từng metric
-- Cho phép thay đổi rule / weight / bật tắt signal mà KHÔNG cần sửa code
-- =====================================================
CREATE TABLE dim_metric (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,

    metric_code VARCHAR(50) NOT NULL,          -- RSI_OVERBOUGHT, PRICE_ABOVE_MA20…
    metric_name VARCHAR(100) NOT NULL,         -- Tên hiển thị
    description TEXT,                          -- Giải thích logic metric

    indicator_type_id INT NOT NULL,             -- RSI / SMA / BB / PRICE

    window_size INT NOT NULL,                  -- số candle (vd: 1, 3, 20)
    window_unit VARCHAR(10) NOT NULL,          -- HOUR / DAY

    threshold_start DECIMAL(10,4) NULL,        -- điều kiện bắt đầu (vd: 70)
    threshold_end   DECIMAL(10,4) NULL,        -- điều kiện kết thúc (vd: 100)

    direction VARCHAR(20) NOT NULL,             -- ABOVE / BELOW / TREND_UP / EXPAND

    metric_weight DECIMAL(6,2) NOT NULL,       -- trọng số (+ / -)

    is_active TINYINT DEFAULT 1,                -- bật / tắt metric

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_metric_indicator
        FOREIGN KEY (indicator_type_id)
        REFERENCES dim_indicator_type(type_id),

    CONSTRAINT uq_metric_code UNIQUE (metric_code)
);

-- =====================================================
-- FACT: METRIC VALUE (TIME-SERIES FEATURE STORE)
-- Lưu giá trị metric theo từng thời điểm thị trường
-- Dùng cho:
-- - Model input
-- - Signal confirmation
-- - Backtest
-- =====================================================
CREATE TABLE fact_metric_value (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    symbol_id INT NOT NULL,
    interval_id INT NOT NULL,         -- candle base (2h, 4h…)

    calculating_at DATETIME NOT NULL, -- 🔥 thời điểm metric xảy ra (event time)

    metric_id INT NOT NULL,
    metric_value DECIMAL(8,4) NOT NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    -- ================= BUSINESS KEY =================
    UNIQUE(symbol_id, interval_id, calculating_at, metric_id),

    -- ================= PERFORMANCE INDEX =================
    -- phục vụ prediction / confirmation / join theo timestamp
    INDEX idx_metric_value_main (symbol_id, interval_id, calculating_at, metric_id),

    -- ================= FOREIGN KEY =================
    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES dim_interval(interval_id),
    FOREIGN KEY (metric_id) REFERENCES dim_metric(metric_id)
);