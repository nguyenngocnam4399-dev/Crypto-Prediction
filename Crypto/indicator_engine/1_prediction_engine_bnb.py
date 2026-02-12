from pyspark.sql import SparkSession, functions as F

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

SYMBOL_ID = 3        # BNB
INTERVAL_ID = 1      # 2H

# ===== THRESHOLD =====
BUY_THRESHOLD  = 6.0
SELL_THRESHOLD = 3.0
MAX_SCORE      = 10.0

# ===== NO-TRADE GUARD (BNB BẮT BUỘC) =====
CONF_MIN = 0.40
CONF_MAX = 0.60

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("PREDICTION_BNB_2H_SAFE")
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
# LOAD FACT_METRIC_VALUE
# =================================================
metric_df = (
    spark.read.jdbc(url, "fact_metric_value", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
)

# =================================================
# LOAD DIM_METRIC
# =================================================
metric_dim = (
    spark.read.jdbc(url, "dim_metric", properties=jdbc_props)
    .filter(F.col("is_active") == 1)
)

# =================================================
# JOIN METRIC + WEIGHT
# =================================================
df = metric_df.join(metric_dim, "metric_id", "inner")

# =================================================
# AGG METRIC GROUPS (NO NULL)
# =================================================
score_df = (
    df.groupBy("calculating_at")
    .agg(
        # TREND
        F.sum(
            F.when(
                F.col("metric_code").rlike("EMA|ADX"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("metric_trend"),

        # MOMENTUM
        F.sum(
            F.when(
                F.col("metric_code").rlike("MACD|RSI"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("metric_momentum"),

        # VOLUME
        F.sum(
            F.when(
                F.col("metric_code").rlike("VOL"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("metric_volume"),

        # VOLATILITY
        F.sum(
            F.when(
                F.col("metric_code").rlike("BB|ATR"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("metric_volatility")
    )
)

# =================================================
# MARKET SCORE
# =================================================
score_df = score_df.withColumn(
    "market_score",
    F.col("metric_trend")
    + F.col("metric_momentum")
    + F.col("metric_volume")
    + F.col("metric_volatility")
)

# =================================================
# ACTION + CONFIDENCE
# =================================================
prediction_df = (
    score_df
    .withColumn(
        "action",
        F.when(F.col("market_score") >= BUY_THRESHOLD, "BUY")
         .when(F.col("market_score") <= SELL_THRESHOLD, "SELL")
         .otherwise("SIDEWAY")
    )
    .withColumn(
        "confidence_score",
        F.round(F.col("market_score") / F.lit(MAX_SCORE), 4)
    )
)

# =================================================
# NO-TRADE RULE (BNB)
# =================================================
prediction_df = (
    prediction_df
    .withColumn(
        "metric_no_trade",
        F.when(
            (F.col("action") == "SIDEWAY") |
            (F.col("confidence_score") < CONF_MIN) |
            (F.col("confidence_score") > CONF_MAX),
            1
        ).otherwise(0)
    )
    .withColumn(
        "reason_no_trade",
        F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
         .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
         .when(F.col("confidence_score") > CONF_MAX, "OVER_CONF")
    )
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
        F.col("close_time").cast("timestamp").alias("predicting_at"),
        F.col("close_price").alias("close")
    )
)

# =================================================
# FINAL DF
# =================================================
final_df = (
    prediction_df
    .withColumnRenamed("calculating_at", "predicting_at")
    .join(price_df, "predicting_at", "left")
    .select(
        F.lit(SYMBOL_ID).alias("symbol_id"),
        F.lit(INTERVAL_ID).alias("interval_id"),
        "predicting_at",
        "close",
        "action",
        F.round("market_score", 4).alias("market_score"),
        "confidence_score",
        F.round("metric_trend", 4).alias("metric_trend"),
        F.round("metric_momentum", 4).alias("metric_momentum"),
        F.round("metric_volume", 4).alias("metric_volume"),
        F.round("metric_volatility", 4).alias("metric_volatility"),
        "metric_no_trade",
        F.lit(0).alias("metric_conflict"),
        "reason_no_trade"
    )
)

# =================================================
# IDEMPOTENT WRITE (ANTI-JOIN)
# =================================================
existing_keys = (
    spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
    .select(
        "symbol_id",
        "interval_id",
        "predicting_at"
    )
)

final_new = (
    final_df
    .join(
        existing_keys,
        on=["symbol_id", "interval_id", "predicting_at"],
        how="left_anti"
    )
)

if final_new.rdd.isEmpty():
    print("ℹ️ No new BNB prediction rows – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_prediction", properties=jdbc_props)
    )
    print("✅ BNB prediction inserted safely")

spark.stop()


# from pyspark.sql import SparkSession, functions as F

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

# SYMBOL_ID = 3        # BNB
# INTERVAL_ID = 1      # 2H

# # ===== THRESHOLD =====
# BUY_THRESHOLD  = 6.0
# SELL_THRESHOLD = 3.0
# MAX_SCORE      = 10.0

# # ===== NO-TRADE GUARD (BNB BẮT BUỘC) =====
# CONF_MIN = 0.40
# CONF_MAX = 0.60

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_BNB_2H_FIXED_NO_NULL")
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
# # LOAD FACT_METRIC_VALUE
# # =================================================
# metric_df = (
#     spark.read.jdbc(url, "fact_metric_value", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
# )

# # =================================================
# # LOAD DIM_METRIC
# # =================================================
# metric_dim = (
#     spark.read.jdbc(url, "dim_metric", properties=jdbc_props)
#     .filter(F.col("is_active") == 1)
# )

# # =================================================
# # JOIN METRIC + WEIGHT
# # =================================================
# df = metric_df.join(metric_dim, "metric_id", "inner")

# # =================================================
# # AGG METRIC GROUPS (🔥 FIX NULL TRIỆT ĐỂ)
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         # TREND
#         F.sum(
#             F.when(
#                 F.col("metric_code").rlike("EMA|ADX"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_trend"),

#         # MOMENTUM
#         F.sum(
#             F.when(
#                 F.col("metric_code").rlike("MACD|RSI"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_momentum"),

#         # VOLUME
#         F.sum(
#             F.when(
#                 F.col("metric_code").rlike("VOL"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_volume"),

#         # VOLATILITY
#         F.sum(
#             F.when(
#                 F.col("metric_code").rlike("BB|ATR"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_volatility")
#     )
# )

# # =================================================
# # MARKET SCORE (KHÔNG BAO GIỜ NULL)
# # =================================================
# score_df = score_df.withColumn(
#     "market_score",
#     F.col("metric_trend")
#     + F.col("metric_momentum")
#     + F.col("metric_volume")
#     + F.col("metric_volatility")
# )

# # =================================================
# # ACTION + CONFIDENCE
# # =================================================
# prediction_df = (
#     score_df
#     .withColumn(
#         "action",
#         F.when(F.col("market_score") >= BUY_THRESHOLD, "BUY")
#          .when(F.col("market_score") <= SELL_THRESHOLD, "SELL")
#          .otherwise("SIDEWAY")
#     )
#     .withColumn(
#         "confidence_score",
#         F.round(F.col("market_score") / F.lit(MAX_SCORE), 4)
#     )
# )

# # =================================================
# # 🛑 NO-TRADE RULE (BNB)
# # =================================================
# prediction_df = (
#     prediction_df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") == "SIDEWAY") |
#             (F.col("confidence_score") < CONF_MIN) |
#             (F.col("confidence_score") > CONF_MAX),
#             1
#         ).otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
#          .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
#          .when(F.col("confidence_score") > CONF_MAX, "OVER_CONF")
#     )
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
#         F.col("close_time").cast("timestamp").alias("predicting_at"),
#         F.col("close_price").alias("close")
#     )
# )

# # =================================================
# # FINAL FACT_PREDICTION
# # =================================================
# final_df = (
#     prediction_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
#     .select(
#         F.lit(SYMBOL_ID).alias("symbol_id"),
#         F.lit(INTERVAL_ID).alias("interval_id"),
#         "predicting_at",
#         "close",
#         "action",
#         F.round("market_score", 4).alias("market_score"),
#         "confidence_score",
#         F.round("metric_trend", 4).alias("metric_trend"),
#         F.round("metric_momentum", 4).alias("metric_momentum"),
#         F.round("metric_volume", 4).alias("metric_volume"),
#         F.round("metric_volatility", 4).alias("metric_volatility"),
#         "metric_no_trade",
#         F.lit(0).alias("metric_conflict"),
#         "reason_no_trade"
#     )
# )

# # =================================================
# # WRITE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction", properties=jdbc_props)
# )

# print("✅ BNB Prediction DONE — NO NULL, NO TRADE GUARD OK")
# spark.stop()


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

# SYMBOL_ID = 3        # BNB
# INTERVAL_ID = 1      # 2H

# # ===== EDGE TEST =====
# BUY_EDGE  = 5.5
# SELL_EDGE = 5.5

# # ===== CONFIDENCE FILTER =====
# MAX_SCORE = 10.0
# CONF_MIN  = 0.55

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("BNB_2H_BUY_SELL_EDGE_TEST_FIXED")
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
# # LOAD METRIC
# # =================================================
# metric_df = (
#     spark.read.jdbc(url, "fact_metric_value_1", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
# )

# metric_dim = (
#     spark.read.jdbc(url, "dim_metric_1", properties=jdbc_props)
#     .filter(F.col("is_active") == 1)
# )

# df = metric_df.join(metric_dim, "metric_id", "inner")

# # =================================================
# # BUY / SELL SCORE (TÁCH RIÊNG)
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         F.sum(
#             F.when(
#                 F.col("metric_code").startswith("BNB_BUY_"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("buy_score"),

#         F.sum(
#             F.when(
#                 F.col("metric_code").startswith("BNB_SELL_"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("sell_score")
#     )
# )

# # =================================================
# # CONFIDENCE (EDGE STRENGTH)
# # =================================================
# score_df = score_df.withColumn(
#     "confidence_score",
#     F.round(
#         F.greatest(F.col("buy_score"), F.col("sell_score")) / F.lit(MAX_SCORE),
#         4
#     )
# )

# # =================================================
# # ACTION DECISION (NO_TRADE WHEN CONFLICT)
# # =================================================
# prediction_df = (
#     score_df
#     .withColumn(
#         "action",
#         F.when(
#             (F.col("buy_score") >= BUY_EDGE) &
#             (F.col("sell_score") < SELL_EDGE),
#             "BUY"
#         )
#         .when(
#             (F.col("sell_score") >= SELL_EDGE) &
#             (F.col("buy_score") < BUY_EDGE),
#             "SELL"
#         )
#         .otherwise("NO_TRADE")
#     )
# )

# # =================================================
# # NO_TRADE FLAG (CONF + CONFLICT)
# # =================================================
# prediction_df = (
#     prediction_df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") == "NO_TRADE") |
#             (F.col("confidence_score") < CONF_MIN),
#             1
#         ).otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(
#             (F.col("buy_score") >= BUY_EDGE) &
#             (F.col("sell_score") >= SELL_EDGE),
#             "CONFLICT"
#         )
#         .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
#         .otherwise(None)
#     )
# )

# # =================================================
# # MAP BUY / SELL SCORE → market_score (KHÔNG ĐỔI SCHEMA)
# # =================================================
# prediction_df = prediction_df.withColumn(
#     "market_score",
#     F.when(F.col("action") == "BUY",  F.col("buy_score"))
#      .when(F.col("action") == "SELL", -F.col("sell_score"))
#      .otherwise(F.lit(0))
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
#         F.col("close_time").cast("timestamp").alias("predicting_at"),
#         F.col("close_price").alias("close")
#     )
# )

# # =================================================
# # FINAL FACT_PREDICTION (MATCH SCHEMA)
# # =================================================
# final_df = (
#     prediction_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
#     .select(
#         F.lit(SYMBOL_ID).alias("symbol_id"),
#         F.lit(INTERVAL_ID).alias("interval_id"),
#         "predicting_at",
#         "close",
#         "action",
#         F.round("market_score", 4).alias("market_score"),
#         "confidence_score",
#         F.lit(None).cast("decimal(8,4)").alias("metric_trend"),
#         F.lit(None).cast("decimal(8,4)").alias("metric_momentum"),
#         F.lit(None).cast("decimal(8,4)").alias("metric_volume"),
#         F.lit(None).cast("decimal(8,4)").alias("metric_volatility"),
#         "metric_no_trade",
#         F.lit(0).alias("metric_conflict"),
#         "reason_no_trade"
#     )
# )

# # =================================================
# # WRITE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction_1", properties=jdbc_props)
# )

# print("✅ DONE — BUY/SELL EDGE TEST (FIXED, NO SCHEMA CHANGE)")
# spark.stop()
