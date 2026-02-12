# # main.py

# from pyspark.sql import SparkSession, Window
# import sys
# import config

# from core.loader import load_kline
# from core.writer import save_indicators
# from core.registry import get_all


# # ======================================================
# # AUTO LOAD INDICATORS
# # ======================================================

# import indicators.momentum
# import indicators.supertrend
# import indicators.trend_filter
# import indicators.trend_strength
# import indicators.trend_utils
# import indicators.trend_volatility
# import indicators.volume


# # ======================================================
# # SPARK SESSION
# # ======================================================

# spark = (
#     SparkSession.builder
#     .appName("IndicatorEngine")

#     # -------- RAM LIMIT --------
#     .config("spark.driver.memory", "2g")          # RAM cho driver
#     .config("spark.executor.memory", "2g")        # RAM cho executor
#     .config("spark.executor.memoryOverhead", "512g")# overhead (quan trọng)
#     .config("spark.memory.fraction", "0.6")       # vùng dùng cho execution + storage
#     .config("spark.memory.storageFraction", "0.3")

#     # -------- STABILITY --------
#     .config("spark.sql.shuffle.partitions", "8")
#     .config("spark.sql.execution.arrow.pyspark.enabled", "false")

#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")


# # ======================================================
# # PARAMS
# # ======================================================

# symbol = sys.argv[1] if len(sys.argv) > 1 else None
# interval = sys.argv[2] if len(sys.argv) > 2 else None


# # ======================================================
# # LOAD DATA
# # ======================================================

# df = load_kline(spark, symbol, interval)


# # ======================================================
# # BASE WINDOW
# # ======================================================

# w = Window.partitionBy(
#     "symbol_id",
#     "interval_id"
# ).orderBy("close_time")


# # ======================================================
# # RUN INDICATORS
# # ======================================================

# frames = []

# for name, func in get_all().items():

#     if name in config.INDICATORS:

#         res = func(df, w)

#         # Normalize timestamp column
#         if "close_time" in res.columns:
#             res = res.withColumnRenamed("close_time", "timestamp")

#         # Safety: ensure required columns exist
#         required = {"symbol_id", "interval_id", "type_name", "value", "timestamp"}
#         missing = required - set(res.columns)

#         if missing:
#             raise Exception(
#                 f"Indicator {name} missing columns: {missing}"
#             )

#         frames.append(res)


# # ======================================================
# # UNION ALL
# # ======================================================

# if not frames:
#     raise Exception("No indicators were generated. Check config.INDICATORS")

# final = frames[0]

# for f in frames[1:]:
#     final = final.unionByName(f)


# # ======================================================
# # SAVE
# # ======================================================

# save_indicators(final, spark)


# # ======================================================
# # STOP
# # ======================================================

# spark.stop()


# main.py

from pyspark.sql import SparkSession, Window
import sys
import config

from core.loader import load_kline
from core.writer import save_indicators
from core.registry import get_all


# ======================================================
# AUTO LOAD INDICATORS
# ======================================================

import indicators.momentum
import indicators.supertrend
import indicators.trend_filter
import indicators.trend_strength
import indicators.trend_utils
import indicators.trend_volatility
import indicators.volume


# ======================================================
# SPARK SESSION (GIỚI HẠN RAM – AN TOÀN)
# ======================================================

spark = (
    SparkSession.builder
    .appName("IndicatorEngine")

    # -------- RAM LIMIT --------
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.memoryOverhead", "512m")  # ⚠️ FIX: KHÔNG DÙNG 512g
    .config("spark.memory.fraction", "0.6")
    .config("spark.memory.storageFraction", "0.3")

    # -------- STABILITY --------
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ======================================================
# PARAMS
# ======================================================

symbol = sys.argv[1] if len(sys.argv) > 1 else None
interval = sys.argv[2] if len(sys.argv) > 2 else None

if symbol is None:
    raise Exception("Usage: spark-submit main.py <symbol_id> <interval_id>")

symbol_id = int(symbol)
interval_id = int(interval) if interval else None


# ======================================================
# MAP SYMBOL -> INDICATORS
# ======================================================

if symbol_id == 1:
    ACTIVE_INDICATORS = config.INDICATORS_BTC
elif symbol_id == 2:
    ACTIVE_INDICATORS = config.INDICATORS_ETH
elif symbol_id == 3:
    ACTIVE_INDICATORS = config.INDICATORS_BNB
else:
    raise Exception(f"Unsupported symbol_id: {symbol_id}")


# ======================================================
# LOAD DATA
# ======================================================

df = load_kline(spark, symbol_id, interval_id)


# ======================================================
# BASE WINDOW
# ======================================================

w = Window.partitionBy(
    "symbol_id",
    "interval_id"
).orderBy("close_time")


# ======================================================
# RUN INDICATORS
# ======================================================

frames = []

for name, func in get_all().items():

    if name in ACTIVE_INDICATORS:

        res = func(df, w)

        # Normalize timestamp
        if "close_time" in res.columns:
            res = res.withColumnRenamed("close_time", "timestamp")

        # Safety check
        required = {
            "symbol_id",
            "interval_id",
            "type_name",
            "value",
            "timestamp"
        }
        missing = required - set(res.columns)
        if missing:
            raise Exception(
                f"Indicator {name} missing columns: {missing}"
            )

        frames.append(res)


# ======================================================
# UNION ALL
# ======================================================

if not frames:
    raise Exception(
        f"No indicators generated for symbol_id={symbol_id}"
    )

final = frames[0]
for f in frames[1:]:
    final = final.unionByName(f)


# ======================================================
# SAVE
# ======================================================

save_indicators(final, spark)


# ======================================================
# STOP
# ======================================================

spark.stop()
