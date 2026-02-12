from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# =================================================
# CONFIG
# =================================================
DB = {
    "host": "localhost",
    "port": "3306",
    "db": "crypto_dw",
    "user": "spark_user",
    "password": "spark123"
}

SYMBOL_ID   = 3   # BNB
INTERVAL_ID = 1   # 2H

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("MetricValue_BNB_2H_BUY_SELL_SAFE")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =================================================
# JDBC
# =================================================
url = (
    f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

jdbc_props = {
    "user": DB["user"],
    "password": DB["password"],
    "driver": "com.mysql.cj.jdbc.Driver"
}

# =================================================
# LOAD INDICATOR
# =================================================
indicator_df = (
    spark.read.jdbc(url, "fact_indicator", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
)

indicator_dim = spark.read.jdbc(
    url, "dim_indicator_type", properties=jdbc_props
)

# =================================================
# LOAD PRICE
# =================================================
price_df = (
    spark.read.jdbc(url, "fact_kline", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
    .select(
        F.col("close_time").cast("timestamp").alias("timestamp"),
        F.col("close_price").alias("close"),
        F.col("volume")
    )
)

# =================================================
# PIVOT INDICATOR
# =================================================
indicator_pivot = (
    indicator_df
    .join(indicator_dim, "type_id")
    .groupBy("symbol_id", "interval_id", "timestamp")
    .pivot("type_name")
    .agg(F.first("value"))
)

df = indicator_pivot.join(price_df, ["timestamp"], "left")

# =================================================
# WINDOW
# =================================================
w = Window.partitionBy("symbol_id", "interval_id").orderBy("timestamp")

# =================================================
# ===== BUY METRICS (GIỮ NGUYÊN LOGIC) =====
# =================================================
df = df.withColumn(
    "BNB_BUY_PRICE_NEAR_VWAP",
    F.when(F.abs(F.col("VWAP_DEV")) <= 0.003, 1).otherwise(0)
)

df = df.withColumn(
    "BNB_BUY_PRICE_BELOW_VWAP",
    F.when(F.col("VWAP_DEV") < 0, 1).otherwise(0)
)

df = df.withColumn(
    "BNB_BUY_MACD_STABILIZE",
    F.when(F.col("MACD_HIST") > F.lag("MACD_HIST").over(w), 1).otherwise(0)
)

df = df.withColumn(
    "BNB_BUY_MACD_POSITIVE",
    F.when(F.col("MACD_HIST") > 0, 1).otherwise(0)
)

df = df.withColumn(
    "BNB_BUY_LOW_VOL",
    F.when(
        (F.col("BB_WIDTH") >= 0.03) &
        (F.col("BB_WIDTH") <= 0.12),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "BNB_BUY_VOL_CONFIRM",
    F.when(F.col("volume") >= F.col("VOL_MA20"), 1).otherwise(0)
)

df = df.withColumn(
    "BNB_BUY_ABOVE_EMA200",
    F.when(F.col("close") >= F.col("EMA200"), 1).otherwise(0)
)

# =================================================
# ===== SELL METRICS (GIỮ NGUYÊN LOGIC) =====
# =================================================
df = df.withColumn(
    "BNB_SELL_MACD_NEGATIVE",
    F.when(F.col("MACD_HIST") < 0, 1).otherwise(0)
)

df = df.withColumn(
    "BNB_SELL_MACD_FALLING",
    F.when(F.col("MACD_HIST") < F.lag("MACD_HIST").over(w), 1).otherwise(0)
)

df = df.withColumn(
    "BNB_SELL_BREAK_BB_DOWN",
    F.when(F.col("close") < F.col("BB_DN20"), 1).otherwise(0)
)

df = df.withColumn(
    "BNB_SELL_VOL_EXPAND",
    F.when(F.col("BB_WIDTH") >= 0.12, 1).otherwise(0)
)

df = df.withColumn(
    "BNB_SELL_VOL_CONFIRM",
    F.when(F.col("volume") >= F.col("VOL_MA20"), 1).otherwise(0)
)

df = df.withColumn(
    "BNB_SELL_BELOW_EMA200",
    F.when(F.col("close") < F.col("EMA200"), 1).otherwise(0)
)

# =================================================
# UNPIVOT → LONG FORMAT
# =================================================
metric_cols = [
    c for c in df.columns
    if c.startswith("BNB_BUY_") or c.startswith("BNB_SELL_")
]

metric_long = (
    df.select(
        "symbol_id",
        "interval_id",
        F.col("timestamp").alias("calculating_at"),
        F.expr(
            "stack({0}, {1}) as (metric_code, metric_value)".format(
                len(metric_cols),
                ", ".join([f"'{c}', {c}" for c in metric_cols])
            )
        )
    )
)

# =================================================
# MAP METRIC_ID
# =================================================
metric_dim = spark.read.jdbc(url, "dim_metric", properties=jdbc_props)

final_df = (
    metric_long
    .join(metric_dim, "metric_code")
    .select(
        "symbol_id",
        "interval_id",
        "calculating_at",
        "metric_id",
        F.col("metric_value").cast("decimal(8,4)")
    )
)

# =================================================
# IDEMPOTENT WRITE (ANTI-JOIN)
# =================================================
existing_keys = (
    spark.read.jdbc(url, "fact_metric_value", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
    .select(
        "symbol_id",
        "interval_id",
        "calculating_at",
        "metric_id"
    )
)

final_new = (
    final_df
    .join(
        existing_keys,
        on=["symbol_id", "interval_id", "calculating_at", "metric_id"],
        how="left_anti"
    )
)

if final_new.rdd.isEmpty():
    print("ℹ️ No new metric rows – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_metric_value", properties=jdbc_props)
    )
    print("✅ FACT_METRIC_VALUE BNB 2H inserted safely")

spark.stop()



# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.window import Window

# # =================================================
# # CONFIG
# # =================================================
# DB = {
#     "host": "localhost",
#     "port": "3306",
#     "db": "crypto_dw",
#     "user": "spark_user",
#     "password": "spark123"
# }

# SYMBOL_ID = 3      # BNB
# INTERVAL_ID = 1    # 2H

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("MetricValue_BNB_2H_BUY_SELL")
#     .config("spark.sql.shuffle.partitions", "8")
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )
# spark.sparkContext.setLogLevel("WARN")

# # =================================================
# # JDBC
# # =================================================
# url = (
#     f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}"
#     "?useSSL=false&allowPublicKeyRetrieval=true"
# )

# jdbc_props = {
#     "user": DB["user"],
#     "password": DB["password"],
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # =================================================
# # LOAD INDICATOR
# # =================================================
# indicator_df = (
#     spark.read.jdbc(url, "fact_indicator", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
# )

# indicator_dim = spark.read.jdbc(
#     url, "dim_indicator_type", properties=jdbc_props
# )

# # =================================================
# # LOAD PRICE
# # =================================================
# price_df = (
#     spark.read.jdbc(url, "fact_kline", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
#     .select(
#         F.col("close_time").cast("timestamp").alias("timestamp"),
#         F.col("close_price").alias("close"),
#         F.col("volume")
#     )
# )

# # =================================================
# # PIVOT INDICATOR → COLUMN
# # =================================================
# indicator_pivot = (
#     indicator_df
#     .join(indicator_dim, "type_id")
#     .groupBy("symbol_id", "interval_id", "timestamp")
#     .pivot("type_name")
#     .agg(F.first("value"))
# )

# df = indicator_pivot.join(price_df, ["timestamp"], "left")

# # =================================================
# # WINDOW
# # =================================================
# w = Window.partitionBy("symbol_id", "interval_id").orderBy("timestamp")

# # =================================================
# # ===== BUY_SOFT METRICS =====
# # =================================================

# # 1. Price near VWAP
# df = df.withColumn(
#     "BNB_BUY_PRICE_NEAR_VWAP",
#     F.when(F.abs(F.col("VWAP_DEV")) <= 0.003, 1).otherwise(0)
# )

# # 2. Price below VWAP
# df = df.withColumn(
#     "BNB_BUY_PRICE_BELOW_VWAP",
#     F.when(F.col("VWAP_DEV") < 0, 1).otherwise(0)
# )

# # 3. MACD stabilize
# df = df.withColumn(
#     "BNB_BUY_MACD_STABILIZE",
#     F.when(
#         F.col("MACD_HIST") > F.lag("MACD_HIST").over(w),
#         1
#     ).otherwise(0)
# )

# # 4. MACD positive
# df = df.withColumn(
#     "BNB_BUY_MACD_POSITIVE",
#     F.when(F.col("MACD_HIST") > 0, 1).otherwise(0)
# )

# # 5. Low volatility
# df = df.withColumn(
#     "BNB_BUY_LOW_VOL",
#     F.when(
#         (F.col("BB_WIDTH") >= 0.03) &
#         (F.col("BB_WIDTH") <= 0.12),
#         1
#     ).otherwise(0)
# )

# # 6. Volume confirm
# df = df.withColumn(
#     "BNB_BUY_VOL_CONFIRM",
#     F.when(F.col("volume") >= F.col("VOL_MA20"), 1).otherwise(0)
# )

# # 7. Above EMA200 (context)
# df = df.withColumn(
#     "BNB_BUY_ABOVE_EMA200",
#     F.when(F.col("close") >= F.col("EMA200"), 1).otherwise(0)
# )

# # =================================================
# # ===== SELL METRICS =====
# # =================================================


# # 9. MACD negative
# df = df.withColumn(
#     "BNB_SELL_MACD_NEGATIVE",
#     F.when(F.col("MACD_HIST") < 0, 1).otherwise(0)
# )

# # 10. MACD falling
# df = df.withColumn(
#     "BNB_SELL_MACD_FALLING",
#     F.when(
#         F.col("MACD_HIST") < F.lag("MACD_HIST").over(w),
#         1
#     ).otherwise(0)
# )

# # 11. Break lower band
# df = df.withColumn(
#     "BNB_SELL_BREAK_BB_DOWN",
#     F.when(F.col("close") < F.col("BB_DN20"), 1).otherwise(0)
# )

# # 12. Volatility expand
# df = df.withColumn(
#     "BNB_SELL_VOL_EXPAND",
#     F.when(F.col("BB_WIDTH") >= 0.12, 1).otherwise(0)
# )

# # 13. Sell volume confirm
# df = df.withColumn(
#     "BNB_SELL_VOL_CONFIRM",
#     F.when(F.col("volume") >= F.col("VOL_MA20"), 1).otherwise(0)
# )

# # 14. Below EMA200
# df = df.withColumn(
#     "BNB_SELL_BELOW_EMA200",
#     F.when(F.col("close") < F.col("EMA200"), 1).otherwise(0)
# )

# # =================================================
# # UNPIVOT → LONG FORM
# # =================================================
# metric_cols = [c for c in df.columns if c.startswith("BNB_BUY_") or c.startswith("BNB_SELL_")]

# metric_long = (
#     df.select(
#         "symbol_id",
#         "interval_id",
#         F.col("timestamp").alias("calculating_at"),
#         F.expr(
#             "stack({0}, {1}) as (metric_code, metric_value)".format(
#                 len(metric_cols),
#                 ", ".join([f"'{c}', {c}" for c in metric_cols])
#             )
#         )
#     )
# )

# # =================================================
# # MAP METRIC_ID
# # =================================================
# metric_dim = spark.read.jdbc(url, "dim_metric", properties=jdbc_props)

# final_df = (
#     metric_long
#     .join(metric_dim, "metric_code")
#     .select(
#         "symbol_id",
#         "interval_id",
#         "calculating_at",
#         "metric_id",
#         F.col("metric_value").cast("decimal(8,4)")
#     )
# )

# # =================================================
# # WRITE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_metric_value", properties=jdbc_props)
# )

# print("✅ FACT_METRIC_VALUE BNB 2H BUY + SELL DONE")
# spark.stop()
