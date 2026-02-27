-- =====================================================
-- DIMENSION: TAG (phân loại news)
-- =====================================================
CREATE TABLE tag_dim (
    tag_id INT AUTO_INCREMENT PRIMARY KEY,  -- PK
    tag_name VARCHAR(255) UNIQUE            -- Tên tag (Regulation, ETF, Hack…)
);

-- =====================================================
-- FACT: NEWS (dữ liệu bài viết gốc)
-- =====================================================
CREATE TABLE news_fact (
    id INT AUTO_INCREMENT PRIMARY KEY,      -- PK

    title VARCHAR(500) NOT NULL,            -- Tiêu đề bài viết
    url VARCHAR(500) NOT NULL UNIQUE,       -- Link gốc (không trùng)

    sentiment_score FLOAT NOT NULL,         -- Điểm sentiment từ model

    created_date DATETIME NOT NULL,         -- Thời gian publish thực tế

    view_number INT NULL,                   -- Lượt xem (độ phổ biến)

    tag_id INT,                             -- FK → tag_dim (loại news)

    FOREIGN KEY (tag_id) REFERENCES tag_dim(tag_id)
);

-- =====================================================
-- FACT: NEWS ↔ COIN MAPPING
-- 1 news có thể ảnh hưởng nhiều coin
-- =====================================================
CREATE TABLE news_coin_fact (
    news_id INT,                            -- FK → news_fact
    symbol_id INT,                          -- FK → dim_symbol

    confidence FLOAT DEFAULT 1.0,           -- Độ liên quan tới coin

    PRIMARY KEY (news_id, symbol_id)        -- Không trùng mapping
);

-- =====================================================
-- FACT: NEWS SENTIMENT WEIGHTED (feature cho model)
-- Sau khi áp dụng:
-- - độ tin cậy nguồn
-- - trọng số tag
-- - mapping coin
-- =====================================================
CREATE TABLE news_sentiment_weighted_fact (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- PK

    news_id INT NOT NULL,                   -- FK → news_fact
    symbol_id INT NOT NULL,                 -- FK → dim_symbol

    event_time DATETIME NOT NULL,           -- Thời gian publish thực

    raw_sentiment FLOAT,                    -- Điểm sentiment gốc
    sentiment_label VARCHAR(20),            -- POSITIVE | NEGATIVE | NEUTRAL

    confidence FLOAT DEFAULT 1.0,           -- Trọng số theo source / relevance
    tag_weight FLOAT DEFAULT 1.0,           -- Trọng số theo loại news

    weighted_score FLOAT,                   -- sentiment sau khi weight
    final_score FLOAT,                      -- sentiment sau khi clamp / normalize

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, -- Thời gian ETL

    UNIQUE KEY uniq_news_symbol (news_id, symbol_id), -- 1 news / 1 coin

    INDEX idx_news (news_id),               -- Join về news
    INDEX idx_symbol (symbol_id),           -- Lọc theo coin
    INDEX idx_event_time (event_time),      -- Time-series feature

    FOREIGN KEY (news_id) REFERENCES news_fact(id),
    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id)
);

-- =====================================================
-- FACT: NEWS SENTIMENT AGGREGATION (time-window feature)
-- Tổng hợp sentiment theo từng khoảng thời gian cho mỗi symbol
-- =====================================================
CREATE TABLE news_sentiment_agg_fact (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- PK

    symbol_id INT NOT NULL,                 -- FK → dim_symbol

    window_start DATETIME NOT NULL,         -- Thời điểm bắt đầu window
    window_end   DATETIME NOT NULL,         -- Thời điểm kết thúc window

    news_count INT,                         -- Số lượng bài báo trong window
    sentiment_weighted FLOAT,               -- Điểm sentiment có trọng số

    created_at DATETIME,                    -- Thời điểm tạo bản ghi

    UNIQUE(symbol_id, window_start),        -- 1 symbol / 1 window

    -- Tối ưu join với prediction / kline theo time-range
    INDEX idx_news_agg_symbol_window (
        symbol_id,
        window_start,
        window_end
    )
);
