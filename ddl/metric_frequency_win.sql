-- =====================================================
-- FP-GROWTH: FREQUENT WIN PATTERNS
-- Lưu tập itemset xuất hiện thường xuyên trong các lệnh WIN
-- =====================================================
CREATE TABLE fp_growth_win_patterns (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- PK

    items JSON NOT NULL,                    -- Danh sách metric tạo thành pattern

    items_hash VARCHAR(255)                 -- Hash để unique & index nhanh
        GENERATED ALWAYS AS (
            JSON_UNQUOTE(JSON_EXTRACT(items, '$'))
        ) STORED,

    freq INT NOT NULL,                      -- Số lần pattern xuất hiện
    pattern_size INT NOT NULL,              -- Số lượng item trong pattern

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Thời điểm generate

    UNIQUE KEY uk_items_hash (items_hash),  -- Không trùng pattern
    INDEX idx_pattern_size (pattern_size)   -- Filter theo độ dài pattern
);

-- =====================================================
-- FP-GROWTH: ASSOCIATION RULES TỪ WIN PATTERNS
-- antecedent → consequent
-- Dùng để tìm điều kiện dẫn đến trade thắng
-- =====================================================
CREATE TABLE fp_growth_win_rules (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- PK

    antecedent JSON NOT NULL,               -- Điều kiện phía trước (IF)
    consequent JSON NOT NULL,               -- Kết quả (THEN)

    antecedent_hash VARCHAR(255)            -- Hash để unique & join nhanh
        GENERATED ALWAYS AS (
            JSON_UNQUOTE(JSON_EXTRACT(antecedent, '$'))
        ) STORED,

    consequent_hash VARCHAR(255)
        GENERATED ALWAYS AS (
            JSON_UNQUOTE(JSON_EXTRACT(consequent, '$'))
        ) STORED,

    confidence FLOAT NOT NULL,              -- Xác suất WIN khi có antecedent
    lift FLOAT NOT NULL,                    -- Độ mạnh của rule (>1 là tốt)
    support FLOAT NOT NULL,                 -- Tần suất xuất hiện trong dataset

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Thời điểm generate

    UNIQUE KEY uk_rule (antecedent_hash, consequent_hash), -- Không trùng rule

    INDEX idx_confidence (confidence),      -- Filter rule mạnh
    INDEX idx_lift (lift)                   -- Filter rule có giá trị
);