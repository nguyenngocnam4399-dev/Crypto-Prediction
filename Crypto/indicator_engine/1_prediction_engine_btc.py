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

# SYMBOL_ID = 1        # BTC
# INTERVAL_ID = 1      # 2H

# # BTC thresholds (rút từ metric bạn gửi)
# BUY_STRONG = 2.8     # BUY rất khó
# SELL_STRONG = 2.0
# MAX_SCORE = 6.0

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_BTC_2H")
#     .config("spark.sql.shuffle.partitions", "8")
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# # =================================================
# # JDBC
# # =================================================
# url = f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}?useSSL=false"
# jdbc_props = {
#     "user": DB["user"],
#     "password": DB["password"],
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # =================================================
# # LOAD METRIC VALUE + DIM
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
# # AGG BUY / SELL SCORE
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         # BUY
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("BTC_BUY_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("buy_score"),

#         # SELL
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("BTC_SELL_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("sell_score"),

#         # TREND GUARD
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("BTC_ADX_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("trend_score")
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

# df = (
#     score_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
# )

# # =================================================
# # ACTION LOGIC (BTC STYLE)
# # =================================================
# df = (
#     df
#     .withColumn(
#         "action",
#         F.when(
#             (F.col("buy_score") >= BUY_STRONG) &
#             (F.col("buy_score") > F.col("sell_score") + 0.8),
#             "BUY"
#         ).when(
#             (F.col("sell_score") >= SELL_STRONG) &
#             (F.col("sell_score") > F.col("buy_score")),
#             "SELL"
#         ).otherwise("SIDEWAY")
#     )
#     .withColumn(
#         "confidence_score",
#         F.round(
#             F.greatest("buy_score", "sell_score") / F.lit(MAX_SCORE),
#             4
#         )
#     )
#     .withColumn(
#         "metric_no_trade",
#         F.when(F.col("action") == "SIDEWAY", 1).otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("action") == "SIDEWAY", "NO_BTC_EDGE")
#     )
# )

# # =================================================
# # FINAL SELECT
# # =================================================
# final_df = df.select(
#     F.lit(SYMBOL_ID).alias("symbol_id"),
#     F.lit(INTERVAL_ID).alias("interval_id"),
#     "predicting_at",
#     "close",
#     "action",
#     F.round(F.abs(F.col("buy_score") - F.col("sell_score")), 4).alias("market_score"),
#     "confidence_score",
#     F.round("trend_score", 4).alias("metric_trend"),
#     F.round("buy_score", 4).alias("metric_momentum"),
#     F.round("sell_score", 4).alias("metric_volatility"),
#     F.lit(0).alias("metric_volume"),
#     "metric_no_trade",
#     F.lit(0).alias("metric_conflict"),
#     "reason_no_trade"
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

# print("✅ PREDICTION BTC 2H DONE")
# spark.stop()


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

# SYMBOL_ID = 1        # BTC
# INTERVAL_ID = 1      # 2H

# # ---- thresholds (đã walk-forward)
# BUY_THRESHOLD  = 2.0
# SELL_THRESHOLD = 2.0
# EDGE_GAP       = 0.8

# CONF_MIN = 0.65
# CONF_MAX = 0.85

# MAX_SCORE = 5.0

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_BTC_2H_V5")
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
# # LOAD METRIC VALUE
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
# # AGG BUY / SELL / REGIME SCORE
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         # BUY
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("BTC_BUY_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("buy_score"),

#         # SELL
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("BTC_SELL_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("sell_score"),

#         # REGIME
#         F.max(
#             F.when(
#                 F.col("metric_code") == "BTC_ADX_STRONG_2H",
#                 F.col("metric_value")
#             ).otherwise(0)
#         ).alias("adx_ok")
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

# df = (
#     score_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
# )

# # =================================================
# # ACTION LOGIC (EDGE + REGIME)
# # =================================================
# df = (
#     df
#     .withColumn(
#         "action",
#         F.when(
#             (F.col("adx_ok") == 1) &
#             (F.col("buy_score") >= BUY_THRESHOLD) &
#             (F.col("buy_score") - F.col("sell_score") >= EDGE_GAP),
#             "BUY"
#         )
#         .when(
#             (F.col("adx_ok") == 1) &
#             (F.col("sell_score") >= SELL_THRESHOLD) &
#             (F.col("sell_score") - F.col("buy_score") >= EDGE_GAP),
#             "SELL"
#         )
#         .otherwise("SIDEWAY")
#     )
#     .withColumn(
#         "confidence_score",
#         F.round(
#             F.greatest("buy_score", "sell_score") / F.lit(MAX_SCORE),
#             4
#         )
#     )
# )

# # =================================================
# # CONFIDENCE GUARD
# # =================================================
# df = (
#     df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") != "SIDEWAY") & (
#                 (F.col("confidence_score") < CONF_MIN) |
#                 (F.col("confidence_score") > CONF_MAX)
#             ),
#             1
#         )
#         .when(F.col("action") == "SIDEWAY", 1)
#         .otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
#          .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
#          .when(F.col("confidence_score") > CONF_MAX, "OVER_CONF")
#     )
# )

# # =================================================
# # FINAL SELECT
# # =================================================
# final_df = df.select(
#     F.lit(SYMBOL_ID).alias("symbol_id"),
#     F.lit(INTERVAL_ID).alias("interval_id"),
#     "predicting_at",
#     "close",
#     "action",

#     F.round(
#         F.abs(F.col("buy_score") - F.col("sell_score")),
#         4
#     ).alias("market_score"),

#     "confidence_score",

#     F.round("buy_score", 4).alias("metric_momentum"),
#     F.round("sell_score", 4).alias("metric_volatility"),

#     F.lit(0).alias("metric_trend"),
#     F.lit(0).alias("metric_volume"),

#     "metric_no_trade",
#     F.lit(0).alias("metric_conflict"),
#     "reason_no_trade"
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

# print("✅ PREDICTION BTC 2H V5 DONE")
# spark.stop()

"""------------------------tốt------------------------------------"""


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

# SYMBOL_ID = 1        # BTC
# INTERVAL_ID = 1      # 2H

# BUY_THRESHOLD  = 2.0
# SELL_THRESHOLD = 2.0
# EDGE_MIN = 1.0
# MAX_SCORE = 5.0

# CONF_MIN = 0.63
# CONF_MAX = 0.8

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_BTC_2H_NO_TRADE_FIXED")
#     .config("spark.sql.shuffle.partitions", "8")
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )
# spark.sparkContext.setLogLevel("WARN")

# # =================================================
# # JDBC
# # =================================================
# url = f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}?useSSL=false"
# jdbc_props = {
#     "user": DB["user"],
#     "password": DB["password"],
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # =================================================
# # LOAD METRIC VALUE + DIM
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
# # AGG BUY / SELL SCORE
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         F.sum(
#             F.when(F.col("metric_code").like("%BUY_%"),
#                    F.col("metric_value") * F.col("metric_weight"))
#             .otherwise(0)
#         ).alias("buy_score"),

#         F.sum(
#             F.when(F.col("metric_code").like("%SELL_%"),
#                    F.col("metric_value") * F.col("metric_weight"))
#             .otherwise(0)
#         ).alias("sell_score")
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

# df = (
#     score_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
# )

# # =================================================
# # STEP 1 — RAW ACTION + EDGE
# # =================================================
# df = (
#     df
#     .withColumn("edge", F.abs(F.col("buy_score") - F.col("sell_score")))
#     .withColumn(
#         "action",
#         F.when(
#             (F.col("buy_score") >= BUY_THRESHOLD) &
#             (F.col("buy_score") > F.col("sell_score")),
#             "BUY"
#         ).when(
#             (F.col("sell_score") >= SELL_THRESHOLD) &
#             (F.col("sell_score") > F.col("buy_score")),
#             "SELL"
#         ).otherwise("SIDEWAY")
#     )
#     .withColumn(
#         "confidence_score",
#         F.round(F.greatest("buy_score", "sell_score") / F.lit(MAX_SCORE), 4)
#     )
# )

# # =================================================
# # 🔥 STEP 2 — CONFLICT DETECTION (MỚI)
# # =================================================
# df = df.withColumn(
#     "metric_conflict",
#     F.when(
#         (F.col("buy_score") >= BUY_THRESHOLD) &
#         (F.col("sell_score") >= SELL_THRESHOLD),
#         1
#     ).otherwise(0)
# )

# # =================================================
# # 🔥 STEP 3 — NO-TRADE MASTER RULE (CHUẨN HOÁ)
# # =================================================
# df = (
#     df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") == "SIDEWAY") |
#             (F.col("confidence_score") < CONF_MIN) |
#             (F.col("confidence_score") > CONF_MAX) |
#             (F.col("edge") < EDGE_MIN) |
#             (F.col("metric_conflict") == 1),
#             1
#         ).otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
#          .when(F.col("edge") < EDGE_MIN, "WEAK_EDGE")
#          .when(F.col("metric_conflict") == 1, "CONFLICT")
#          .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
#          .when(F.col("confidence_score") > CONF_MAX, "OVER_CONF")
#     )
# )

# # =================================================
# # FINAL SELECT (KHÔNG ĐỔI SCHEMA)
# # =================================================
# final_df = df.select(
#     F.lit(SYMBOL_ID).alias("symbol_id"),
#     F.lit(INTERVAL_ID).alias("interval_id"),
#     "predicting_at",
#     "close",
#     "action",
#     F.round("edge", 4).alias("market_score"),
#     "confidence_score",
#     F.round("buy_score", 4).alias("metric_momentum"),
#     F.round("sell_score", 4).alias("metric_volatility"),
#     F.lit(0).alias("metric_trend"),
#     F.lit(0).alias("metric_volume"),
#     "metric_no_trade",
#     "metric_conflict",
#     "reason_no_trade"
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

# print("✅ PREDICTION BTC 2H WITH FIXED NO-TRADE DONE")
# spark.stop()





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

# SYMBOL_ID   = 1        # BTC
# INTERVAL_ID = 1        # 2H

# # ===== SCORE =====
# BUY_THRESHOLD  = 2.0
# SELL_THRESHOLD = 2.0
# MAX_SCORE      = 5.0

# # ===== EDGE =====
# EDGE_MIN_SELL  = 1.0
# EDGE_MIN_BUY   = 1.0      # 🔥 hạ từ 1.4 → BUY có lại
# STRONG_EDGE    = 2.5

# # ===== CONFIDENCE =====
# CONF_BUY_MIN  = 0.62      # 🔥 mở BUY pullback
# CONF_BUY_MAX  = 0.80

# CONF_SELL_WEAK_MIN   = 0.55
# CONF_SELL_STRONG_MIN = 0.45
# CONF_SELL_MAX        = 0.90

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_BTC_2H_BUY_SELL_V6")
#     .config("spark.sql.shuffle.partitions", "8")
#     .config("spark.sql.session.timeZone", "UTC")
#     .getOrCreate()
# )
# spark.sparkContext.setLogLevel("WARN")

# # =================================================
# # JDBC
# # =================================================
# url = f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}?useSSL=false"
# jdbc_props = {
#     "user": DB["user"],
#     "password": DB["password"],
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # =================================================
# # LOAD METRIC
# # =================================================
# metric_df = (
#     spark.read.jdbc(url, "fact_metric_value", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
# )

# metric_dim = (
#     spark.read.jdbc(url, "dim_metric", properties=jdbc_props)
#     .filter(F.col("is_active") == 1)
# )

# df = metric_df.join(metric_dim, "metric_id", "inner")

# # =================================================
# # AGG BUY / SELL SCORE
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         F.sum(
#             F.when(F.col("metric_code").like("%BUY_%"),
#                    F.col("metric_value") * F.col("metric_weight"))
#             .otherwise(0)
#         ).alias("buy_score"),

#         F.sum(
#             F.when(F.col("metric_code").like("%SELL_%"),
#                    F.col("metric_value") * F.col("metric_weight"))
#             .otherwise(0)
#         ).alias("sell_score")
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

# df = (
#     score_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
# )

# # =================================================
# # STEP 1 — ACTION + EDGE
# # =================================================
# df = (
#     df
#     .withColumn("edge", F.abs(F.col("buy_score") - F.col("sell_score")))
#     .withColumn(
#         "action",
#         F.when(
#             (F.col("buy_score") >= BUY_THRESHOLD) &
#             (F.col("buy_score") > F.col("sell_score")),
#             "BUY"
#         ).when(
#             (F.col("sell_score") >= SELL_THRESHOLD) &
#             (F.col("sell_score") > F.col("buy_score")),
#             "SELL"
#         ).otherwise("SIDEWAY")
#     )
#     .withColumn(
#         "confidence_score",
#         F.round(F.greatest("buy_score", "sell_score") / F.lit(MAX_SCORE), 4)
#     )
# )

# # =================================================
# # STEP 2 — CONFLICT
# # =================================================
# df = df.withColumn(
#     "metric_conflict",
#     F.when(
#         (F.col("buy_score") >= BUY_THRESHOLD) &
#         (F.col("sell_score") >= SELL_THRESHOLD),
#         1
#     ).otherwise(0)
# )

# # =================================================
# # STEP 3 — EDGE REQUIREMENT
# # =================================================
# df = df.withColumn(
#     "edge_min_required",
#     F.when(F.col("action") == "BUY", F.lit(EDGE_MIN_BUY))
#      .otherwise(F.lit(EDGE_MIN_SELL))
# )

# # =================================================
# # STEP 4 — NO-TRADE MASTER RULE (V6)
# # =================================================
# df = (
#     df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") == "SIDEWAY") |
#             (F.col("metric_conflict") == 1) |
#             (F.col("edge") < F.col("edge_min_required")),
#             1
#         )
#         # BUY filter (mở pullback)
#         .when(
#             (F.col("action") == "BUY") &
#             (
#                 (F.col("confidence_score") < CONF_BUY_MIN) |
#                 (F.col("confidence_score") > CONF_BUY_MAX)
#             ),
#             1
#         )
#         # SELL strong
#         .when(
#             (F.col("action") == "SELL") &
#             (F.col("edge") >= STRONG_EDGE) &
#             (F.col("confidence_score") < CONF_SELL_STRONG_MIN),
#             1
#         )
#         # SELL weak
#         .when(
#             (F.col("action") == "SELL") &
#             (F.col("edge") < STRONG_EDGE) &
#             (
#                 (F.col("confidence_score") < CONF_SELL_WEAK_MIN) |
#                 (F.col("confidence_score") > CONF_SELL_MAX)
#             ),
#             1
#         )
#         .otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
#          .when(F.col("edge") < F.col("edge_min_required"), "WEAK_EDGE")
#          .when(F.col("metric_conflict") == 1, "CONFLICT")
#          .when(F.col("action") == "BUY", "BUY_CONF_FILTER")
#          .when(F.col("edge") >= STRONG_EDGE, "SELL_STRONG_CONF")
#          .otherwise("SELL_WEAK_CONF")
#     )
# )

# # =================================================
# # FINAL SELECT (GIỮ NGUYÊN SCHEMA)
# # =================================================
# final_df = df.select(
#     F.lit(SYMBOL_ID).alias("symbol_id"),
#     F.lit(INTERVAL_ID).alias("interval_id"),
#     "predicting_at",
#     "close",
#     "action",
#     F.round("edge", 4).alias("market_score"),
#     "confidence_score",
#     F.round("buy_score", 4).alias("metric_momentum"),
#     F.round("sell_score", 4).alias("metric_volatility"),
#     F.lit(0).alias("metric_trend"),
#     F.lit(0).alias("metric_volume"),
#     "metric_no_trade",
#     "metric_conflict",
#     "reason_no_trade"
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

# print("✅ PREDICTION_BTC_2H_BUY_SELL_V6 DONE")
# spark.stop()



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

SYMBOL_ID   = 1        # BTC
INTERVAL_ID = 1        # 2H

# ===== SCORE =====
BUY_THRESHOLD  = 2.0
SELL_THRESHOLD = 2.0
MAX_SCORE      = 5.0

# ===== EDGE =====
EDGE_MIN_SELL  = 1.0
EDGE_MIN_BUY   = 1.0
STRONG_EDGE    = 2.5

# ===== CONFIDENCE =====
CONF_BUY_MIN  = 0.62
CONF_BUY_MAX  = 0.80

CONF_SELL_WEAK_MIN   = 0.55
CONF_SELL_STRONG_MIN = 0.45
CONF_SELL_MAX        = 0.90

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("PREDICTION_BTC_2H_BUY_SELL_V6_SAFE")
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
# LOAD METRIC
# =================================================
metric_df = (
    spark.read.jdbc(url, "fact_metric_value", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
)

metric_dim = (
    spark.read.jdbc(url, "dim_metric", properties=jdbc_props)
    .filter(F.col("is_active") == 1)
)

df = metric_df.join(metric_dim, "metric_id", "inner")

# =================================================
# AGG BUY / SELL SCORE (GIỮ NGUYÊN LOGIC)
# =================================================
score_df = (
    df.groupBy("calculating_at")
    .agg(
        F.sum(
            F.when(
                F.col("metric_code").like("%BUY_%"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("buy_score"),

        F.sum(
            F.when(
                F.col("metric_code").like("%SELL_%"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("sell_score")
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

df = (
    score_df
    .withColumnRenamed("calculating_at", "predicting_at")
    .join(price_df, "predicting_at", "left")
)

# =================================================
# STEP 1 — ACTION + EDGE
# =================================================
df = (
    df
    .withColumn("edge", F.abs(F.col("buy_score") - F.col("sell_score")))
    .withColumn(
        "action",
        F.when(
            (F.col("buy_score") >= BUY_THRESHOLD) &
            (F.col("buy_score") > F.col("sell_score")),
            "BUY"
        )
        .when(
            (F.col("sell_score") >= SELL_THRESHOLD) &
            (F.col("sell_score") > F.col("buy_score")),
            "SELL"
        )
        .otherwise("SIDEWAY")
    )
    .withColumn(
        "confidence_score",
        F.round(
            F.greatest("buy_score", "sell_score") / F.lit(MAX_SCORE),
            4
        )
    )
)

# =================================================
# STEP 2 — CONFLICT
# =================================================
df = df.withColumn(
    "metric_conflict",
    F.when(
        (F.col("buy_score") >= BUY_THRESHOLD) &
        (F.col("sell_score") >= SELL_THRESHOLD),
        1
    ).otherwise(0)
)

# =================================================
# STEP 3 — EDGE REQUIREMENT
# =================================================
df = df.withColumn(
    "edge_min_required",
    F.when(F.col("action") == "BUY", F.lit(EDGE_MIN_BUY))
     .otherwise(F.lit(EDGE_MIN_SELL))
)

# =================================================
# STEP 4 — NO-TRADE MASTER RULE (V6 – GIỮ NGUYÊN)
# =================================================
df = (
    df
    .withColumn(
        "metric_no_trade",
        F.when(
            (F.col("action") == "SIDEWAY") |
            (F.col("metric_conflict") == 1) |
            (F.col("edge") < F.col("edge_min_required")),
            1
        )
        # BUY filter
        .when(
            (F.col("action") == "BUY") &
            (
                (F.col("confidence_score") < CONF_BUY_MIN) |
                (F.col("confidence_score") > CONF_BUY_MAX)
            ),
            1
        )
        # SELL strong
        .when(
            (F.col("action") == "SELL") &
            (F.col("edge") >= STRONG_EDGE) &
            (F.col("confidence_score") < CONF_SELL_STRONG_MIN),
            1
        )
        # SELL weak
        .when(
            (F.col("action") == "SELL") &
            (F.col("edge") < STRONG_EDGE) &
            (
                (F.col("confidence_score") < CONF_SELL_WEAK_MIN) |
                (F.col("confidence_score") > CONF_SELL_MAX)
            ),
            1
        )
        .otherwise(0)
    )
    .withColumn(
        "reason_no_trade",
        F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
         .when(F.col("edge") < F.col("edge_min_required"), "WEAK_EDGE")
         .when(F.col("metric_conflict") == 1, "CONFLICT")
         .when(F.col("action") == "BUY", "BUY_CONF_FILTER")
         .when(F.col("edge") >= STRONG_EDGE, "SELL_STRONG_CONF")
         .otherwise("SELL_WEAK_CONF")
    )
)

# =================================================
# FINAL SCHEMA (KHÔNG ĐỔI)
# =================================================
final_df = df.select(
    F.lit(SYMBOL_ID).alias("symbol_id"),
    F.lit(INTERVAL_ID).alias("interval_id"),
    "predicting_at",
    "close",
    "action",
    F.round("edge", 4).alias("market_score"),
    "confidence_score",
    F.round("buy_score", 4).alias("metric_momentum"),
    F.round("sell_score", 4).alias("metric_volatility"),
    F.lit(0).alias("metric_trend"),
    F.lit(0).alias("metric_volume"),
    "metric_no_trade",
    "metric_conflict",
    "reason_no_trade"
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
    print("ℹ️ No new BTC prediction rows – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_prediction", properties=jdbc_props)
    )
    print("✅ BTC prediction inserted safely")

spark.stop()
