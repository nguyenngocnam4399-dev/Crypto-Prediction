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

SYMBOL_ID = 1      # BTC
INTERVAL_ID = 1    # 2H

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("Metric_BTC_2H_SAFE")
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
        F.col("close_price").alias("close")
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
# ===== TREND CONTEXT =====
# =================================================
df = df.withColumn(
    "BTC_ADX_STRONG_2H",
    F.when(F.col("ADX14") >= 20, 1).otherwise(0)
)

# =================================================
# ===== BUY METRIC =====
# =================================================
df = df.withColumn(
    "BTC_BUY_MACD_BULL_2H",
    F.when(
        (F.col("MACD") > 0) &
        (F.col("MACD") > F.lag("MACD").over(w)),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "BTC_BUY_RSI_BULL_2H",
    F.when(F.col("RSI14") > 50, 1).otherwise(0)
)

df = df.withColumn(
    "BTC_BUY_VOL_OK_2H",
    F.when(F.col("BB_WIDTH") > 0.03, 1).otherwise(0)
)

# =================================================
# ===== SELL METRIC =====
# =================================================
df = df.withColumn(
    "BTC_SELL_TREND_DOWN_2H",
    F.when(
        F.col("EMA200") < F.lag("EMA200").over(w),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "BTC_SELL_ADX_WEAK_2H",
    F.when(F.col("ADX14") < 20, 1).otherwise(0)
)

df = df.withColumn(
    "BTC_SELL_MACD_BEAR_2H",
    F.when(
        (F.col("MACD") < 0) &
        (F.col("MACD") < F.lag("MACD").over(w)),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "BTC_SELL_MACD_HIST_DOWN_2H",
    F.when(
        F.col("MACD_HIST") < F.lag("MACD_HIST").over(w),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "BTC_SELL_RSI_BEAR_2H",
    F.when(F.col("RSI14") < 45, 1).otherwise(0)
)

df = df.withColumn(
    "BTC_SELL_FAIL_BB_UP_2H",
    F.when(
        (F.col("close") < F.col("BB_UP20")) &
        (F.lag("close").over(w) >= F.lag("BB_UP20").over(w)),
        1
    ).otherwise(0)
)

# =================================================
# UNPIVOT METRIC
# =================================================
metric_cols = [c for c in df.columns if c.startswith("BTC_")]

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
# ===== ANTI-JOIN (SAFE CORE) =====
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
    print("ℹ️ No new BTC metric rows – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_metric_value", properties=jdbc_props)
    )
    print("✅ Inserted new BTC metric rows")

spark.stop()
print("🏁 METRIC BTC 2H DONE (SAFE)")



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

# SYMBOL_ID = 1      # BTC
# INTERVAL_ID = 1    # 2H

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("Metric_BTC_2H_FIXED")
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
#         F.col("close_price").alias("close")
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
# # ===== TREND CONTEXT =====
# # =================================================

# # ADX >= 20 → có trend thật
# df = df.withColumn(
#     "BTC_ADX_STRONG_2H",
#     F.when(F.col("ADX14") >= 20, 1).otherwise(0)
# )

# # =================================================
# # ===== BUY METRIC =====
# # =================================================

# # MACD bullish (momentum lên)
# df = df.withColumn(
#     "BTC_BUY_MACD_BULL_2H",
#     F.when(
#         (F.col("MACD") > 0) &
#         (F.col("MACD") > F.lag("MACD").over(w)),
#         1
#     ).otherwise(0)
# )

# # RSI > 50 → bullish regime
# df = df.withColumn(
#     "BTC_BUY_RSI_BULL_2H",
#     F.when(F.col("RSI14") > 50, 1).otherwise(0)
# )

# # BB_WIDTH > 0.03 → không bị squeeze
# df = df.withColumn(
#     "BTC_BUY_VOL_OK_2H",
#     F.when(F.col("BB_WIDTH") > 0.03, 1).otherwise(0)
# )

# # =================================================
# # ===== SELL METRIC =====
# # =================================================

# # EMA200 dốc xuống → bias SELL
# df = df.withColumn(
#     "BTC_SELL_TREND_DOWN_2H",
#     F.when(
#         F.col("EMA200") < F.lag("EMA200").over(w),
#         1
#     ).otherwise(0)
# )

# # ADX < 20 → trend yếu
# df = df.withColumn(
#     "BTC_SELL_ADX_WEAK_2H",
#     F.when(F.col("ADX14") < 20, 1).otherwise(0)
# )

# # MACD bearish (momentum xuống)
# df = df.withColumn(
#     "BTC_SELL_MACD_BEAR_2H",
#     F.when(
#         (F.col("MACD") < 0) &
#         (F.col("MACD") < F.lag("MACD").over(w)),
#         1
#     ).otherwise(0)
# )

# # MACD_HIST giảm
# df = df.withColumn(
#     "BTC_SELL_MACD_HIST_DOWN_2H",
#     F.when(
#         F.col("MACD_HIST") < F.lag("MACD_HIST").over(w),
#         1
#     ).otherwise(0)
# )

# # RSI < 45 → bearish regime
# df = df.withColumn(
#     "BTC_SELL_RSI_BEAR_2H",
#     F.when(F.col("RSI14") < 45, 1).otherwise(0)
# )

# # Fail tại BB_UP (breakout thất bại)
# df = df.withColumn(
#     "BTC_SELL_FAIL_BB_UP_2H",
#     F.when(
#         (F.col("close") < F.col("BB_UP20")) &
#         (F.lag("close").over(w) >= F.lag("BB_UP20").over(w)),
#         1
#     ).otherwise(0)
# )

# # =================================================
# # UNPIVOT METRIC
# # =================================================
# metric_cols = [c for c in df.columns if c.startswith("BTC_")]

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

# print("✅ METRIC BTC 2H FIXED DONE")
# spark.stop()
