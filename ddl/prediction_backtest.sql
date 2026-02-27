-- =====================================================
-- FACT: PREDICTION (tín hiệu model sinh ra)
-- 1 symbol + 1 timeframe + 1 signal_time + 1 signal_type
-- =====================================================
CREATE TABLE fact_prediction (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- PK

    symbol_id   BIGINT NOT NULL,            -- FK → symbol
    interval_id INT    NOT NULL,            -- FK → timeframe

    signal_time DATETIME NOT NULL,          -- Thời điểm phát tín hiệu

    signal_type VARCHAR(20) NOT NULL,       -- BUY | SELL | SIDEWAY

    entry_price       DOUBLE NULL,          -- Giá vào lệnh
    take_profit_price DOUBLE NULL,          -- Giá TP
    stop_loss_price   DOUBLE NULL,          -- Giá SL

    market_score DOUBLE NOT NULL,           -- Điểm tổng hợp market condition
    market_bias  VARCHAR(20) NOT NULL,      -- BULLISH | BEARISH | SIDEWAY

    status VARCHAR(20) NOT NULL,            -- PENDING | WIN | LOSS | EXPIRED

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Thời điểm tạo signal

    UNIQUE KEY uk_prediction (symbol_id, interval_id, signal_time, signal_type), -- Không trùng tín hiệu

    INDEX idx_pred_symbol (symbol_id),      -- Lọc theo symbol
    INDEX idx_pred_time (signal_time),      -- Query theo thời gian
    INDEX idx_pred_type (signal_type),      -- Thống kê theo loại signal
    INDEX idx_pred_status (status)          -- Tracking trạng thái
);

-- =====================================================
-- FACT: PREDICTION RESULT (kết quả sau khi đóng lệnh)
-- 1 prediction → 1 kết quả
-- =====================================================
CREATE TABLE fact_prediction_result (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- PK

    prediction_id BIGINT NOT NULL,          -- FK → fact_prediction

    result_status VARCHAR(20) NOT NULL,     -- WIN | LOSS | EXPIRED

    exit_price DOUBLE,                      -- Giá đóng lệnh
    pnl_pct DOUBLE,                         -- % lời/lỗ

    confirmed_at DATETIME,                  -- Thời điểm xác nhận kết quả
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Thời điểm ghi nhận

    UNIQUE(prediction_id)                   -- Mỗi prediction chỉ có 1 kết quả
);