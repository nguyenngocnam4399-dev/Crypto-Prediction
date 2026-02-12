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

# SYMBOL_ID = 2        # ETH
# INTERVAL_ID = 1      # 2H

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("MetricValue_ETH_2H_FIXED")
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
#     "&useUnicode=true&characterEncoding=utf8"
# )

# jdbc_props = {
#     "user": DB["user"],
#     "password": DB["password"],
#     "driver": "com.mysql.cj.jdbc.Driver",
#     "rewriteBatchedStatements": "true"
# }

# # =================================================
# # LOAD INDICATOR FACT
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
# # LOAD PRICE (CLOSE, VOLUME)
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
# # PIVOT INDICATOR
# # =================================================
# indicator_pivot = (
#     indicator_df
#     .join(indicator_dim, "type_id")
#     .groupBy("symbol_id", "interval_id", "timestamp")
#     .pivot("type_name")
#     .agg(F.first("value"))
# )

# df = indicator_pivot.join(price_df, "timestamp", "left")

# # =================================================
# # WINDOW
# # =================================================
# w = Window.partitionBy("symbol_id", "interval_id").orderBy("timestamp")

# # =================================================
# # ===== TREND GUARD =====
# # =================================================
# df = df.withColumn(
#     "ETH_TREND_GUARD_2H",
#     F.when(F.col("ADX14") < 35, 1).otherwise(0)
# )

# # =================================================
# # ===== BUY METRICS =====
# # =================================================

# # VWAP deviation (undervalued)
# df = df.withColumn(
#     "ETH_BUY_VWAP_DEV_OK_2H",
#     F.when(
#         (F.col("VWAP_DEV") >= -0.008) &
#         (F.col("VWAP_DEV") <= -0.002),
#         1
#     ).otherwise(0)
# )

# # MACD histogram recovering
# df = df.withColumn(
#     "ETH_BUY_MACD_RECOVERING_2H",
#     F.when(
#         F.col("MACD_HIST") > F.lag("MACD_HIST").over(w),
#         1
#     ).otherwise(0)
# )

# # Volatility healthy
# df = df.withColumn(
#     "ETH_BUY_VOLATILITY_OK_2H",
#     F.when(
#         (F.col("BB_WIDTH") >= 0.04) &
#         (F.col("BB_WIDTH") <= 0.14),
#         1
#     ).otherwise(0)
# )

# # Volume confirm rebound
# df = df.withColumn(
#     "ETH_BUY_VOLUME_CONFIRM_2H",
#     F.when(F.col("volume") >= F.col("VOL_MA20"), 1).otherwise(0)
# )

# # =================================================
# # ===== SELL METRICS =====
# # =================================================

# # VWAP deviation (overvalued)
# df = df.withColumn(
#     "ETH_SELL_VWAP_DEV_HIGH_2H",
#     F.when(
#         (F.col("VWAP_DEV") >= 0.002) &
#         (F.col("VWAP_DEV") <= 0.008),
#         1
#     ).otherwise(0)
# )

# # MACD histogram weakening
# df = df.withColumn(
#     "ETH_SELL_MACD_WEAKENING_2H",
#     F.when(
#         F.col("MACD_HIST") < F.lag("MACD_HIST").over(w),
#         1
#     ).otherwise(0)
# )

# # Volatility healthy
# df = df.withColumn(
#     "ETH_SELL_VOLATILITY_OK_2H",
#     F.when(
#         (F.col("BB_WIDTH") >= 0.04) &
#         (F.col("BB_WIDTH") <= 0.14),
#         1
#     ).otherwise(0)
# )

# # Volume confirm distribution
# df = df.withColumn(
#     "ETH_SELL_VOLUME_CONFIRM_2H",
#     F.when(F.col("volume") < F.col("VOL_MA20"), 1).otherwise(0)
# )

# # =================================================
# # ===== GUARD METRICS (NO SCORE) =====
# # =================================================

# df = df.withColumn(
#     "ETH_GUARD_BUY_BREAKDOWN_2H",
#     F.when(F.col("close") < F.col("BB_DN20"), 1).otherwise(0)
# )

# df = df.withColumn(
#     "ETH_GUARD_SELL_SQUEEZE_2H",
#     F.when(F.col("close") > F.col("BB_UP20"), 1).otherwise(0)
# )

# # =================================================
# # UNPIVOT → LONG FORMAT
# # =================================================
# metric_cols = [c for c in df.columns if c.startswith("ETH_")]

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
# # WRITE FACT_METRIC_VALUE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_metric_value", properties=jdbc_props)
# )

# print("✅ FACT_METRIC_VALUE ETH 2H FIXED DONE")
# spark.stop()


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

SYMBOL_ID = 2        # ETH
INTERVAL_ID = 1     # 2H

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("MetricValue_ETH_2H_SAFE")
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
    "&useUnicode=true&characterEncoding=utf8"
)

jdbc_props = {
    "user": DB["user"],
    "password": DB["password"],
    "driver": "com.mysql.cj.jdbc.Driver",
    "rewriteBatchedStatements": "true"
}

# =================================================
# LOAD INDICATOR FACT
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

df = indicator_pivot.join(price_df, "timestamp", "left")

# =================================================
# WINDOW
# =================================================
w = Window.partitionBy("symbol_id", "interval_id").orderBy("timestamp")

# =================================================
# ===== TREND GUARD =====
# =================================================
df = df.withColumn(
    "ETH_TREND_GUARD_2H",
    F.when(F.col("ADX14") < 35, 1).otherwise(0)
)

# =================================================
# ===== BUY METRICS =====
# =================================================
df = df.withColumn(
    "ETH_BUY_VWAP_DEV_OK_2H",
    F.when(
        (F.col("VWAP_DEV") >= -0.008) &
        (F.col("VWAP_DEV") <= -0.002),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "ETH_BUY_MACD_RECOVERING_2H",
    F.when(
        F.col("MACD_HIST") > F.lag("MACD_HIST").over(w),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "ETH_BUY_VOLATILITY_OK_2H",
    F.when(
        (F.col("BB_WIDTH") >= 0.04) &
        (F.col("BB_WIDTH") <= 0.14),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "ETH_BUY_VOLUME_CONFIRM_2H",
    F.when(F.col("volume") >= F.col("VOL_MA20"), 1).otherwise(0)
)

# =================================================
# ===== SELL METRICS =====
# =================================================
df = df.withColumn(
    "ETH_SELL_VWAP_DEV_HIGH_2H",
    F.when(
        (F.col("VWAP_DEV") >= 0.002) &
        (F.col("VWAP_DEV") <= 0.008),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "ETH_SELL_MACD_WEAKENING_2H",
    F.when(
        F.col("MACD_HIST") < F.lag("MACD_HIST").over(w),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "ETH_SELL_VOLATILITY_OK_2H",
    F.when(
        (F.col("BB_WIDTH") >= 0.04) &
        (F.col("BB_WIDTH") <= 0.14),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "ETH_SELL_VOLUME_CONFIRM_2H",
    F.when(F.col("volume") < F.col("VOL_MA20"), 1).otherwise(0)
)

# =================================================
# ===== GUARD METRICS =====
# =================================================
df = df.withColumn(
    "ETH_GUARD_BUY_BREAKDOWN_2H",
    F.when(F.col("close") < F.col("BB_DN20"), 1).otherwise(0)
)

df = df.withColumn(
    "ETH_GUARD_SELL_SQUEEZE_2H",
    F.when(F.col("close") > F.col("BB_UP20"), 1).otherwise(0)
)

# =================================================
# UNPIVOT
# =================================================
metric_cols = [c for c in df.columns if c.startswith("ETH_")]

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
# ===== ANTI-JOIN (IDEMPOTENT CORE) =====
# =================================================
existing_keys = (
    spark.read.jdbc(url, "fact_metric_value", properties=jdbc_props)
    .select(
        "symbol_id",
        "interval_id",
        "calculating_at",
        "metric_id"
    )
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
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

# =================================================
# WRITE SAFE
# =================================================
if final_new.rdd.isEmpty():
    print("ℹ️ No new metric rows to insert – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_metric_value", properties=jdbc_props)
    )
    print("✅ Inserted new metric rows")

spark.stop()
print("🏁 METRIC VALUE ETH 2H DONE (SAFE)")
