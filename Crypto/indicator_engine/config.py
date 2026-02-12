# config.py

DB_CONFIG = {
    "host": "localhost",
    "port": "3306",
    "user": "spark_user",
    "password": "spark123",
    "database": "crypto_dw"
}

# Indicator bật/tắt tại đây


INDICATORS_BNB = [

    # =================================================
    # TREND CONTEXT (CHUNG – KHÔNG TẠO LỆNH)
    # =================================================
    "EMA200",        # trend lớn
    "ADX14",         # có trend hay không


    # =================================================
    # ===== BUY SOFT (MEAN REVERSION) =====
    # =================================================

    # --- Bias quanh mean ---
    "VWAP_D",        # giá trên/dưới VWAP
    "VWAP_DEV",      # % lệch VWAP

    # --- Momentum bật lên ---
    "MACD",
    "MACD_HIST",

    # --- Volatility lọc nhiễu ---
    "BB_WIDTH",

    # --- Volume xác nhận nhẹ ---
    "VOL_MA20",

    # --- Breakdown volatility ---
    "BB_DN20",
]

INDICATORS_ETH = [

    # =================================================
    # TREND CONTEXT (DÙNG CHUNG – KHÔNG TẠO LỆNH)
    # =================================================

    "EMA200",    # trend lớn / market regime
    "ADX14",     # có trend mạnh hay sideway

    # =================================================
    # BUY (LONG / BUY_SOFT – MEAN REVERSION)
    # =================================================

    # --- Bias quanh mean ---
    "VWAP_D",        # giá dưới VWAP
    "VWAP_DEV",      # lệch âm (undervalued)

    # --- Momentum hồi ---
    "MACD",
    "MACD_HIST",     # momentum cải thiện

    # --- Volatility lọc nhiễu ---
    "BB_WIDTH",      # vol vừa phải

    # --- Volume xác nhận ---
    "VOL_MA20",

    # --- Protection ---
    "BB_DN20",       # tránh BUY khi breakdown

    # --- Protection ---
    "BB_UP20",       # tránh SELL khi squeeze mạnh
]


INDICATORS_BTC = [

    # =================================================
    # TREND CONTEXT (RẤT QUAN TRỌNG VỚI BTC)
    # =================================================
    "EMA200",        # regime chính
    "ADX14",         # có trend thật hay không

    # =================================================
    # MOMENTUM (BTC phản ứng mạnh với momentum)
    # =================================================
    "MACD",
    "MACD_HIST",
    "RSI14",

    # =================================================
    # VOLATILITY (lọc fake break)
    # =================================================
    "BB_WIDTH",

    # =================================================
    # PRICE STRUCTURE
    # =================================================
    "BB_UP20",
    "BB_DN20",

    # =================================================
    # VOLUME (xác nhận breakout)
    # =================================================
    "VOL_MA20",
]
