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

# SYMBOL_ID = 2        # ETH
# INTERVAL_ID = 1      # 2H
# MAX_SCORE = 10.0

# # =================================================
# # BUY_SOFT_V3 – PERCENTILE THRESHOLD (ETH 2H)
# # (derive từ data thật)
# # =================================================
# P60_MOMENTUM = 1.3
# P40_VOL = 2.2
# P55_CONF = 0.59

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("BUY_SOFT_V3_PREDICTION_ETH_2H")
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
# metric_value_df = (
#     spark.read.jdbc(url, "fact_metric_value_1", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
# )

# # =================================================
# # LOAD DIM_METRIC
# # =================================================
# metric_dim_df = (
#     spark.read.jdbc(url, "dim_metric_1", properties=jdbc_props)
#     .filter(F.col("is_active") == 1)
# )

# # =================================================
# # JOIN METRIC
# # =================================================
# df = (
#     metric_value_df.alias("f")
#     .join(
#         metric_dim_df.alias("m"),
#         F.col("f.metric_id") == F.col("m.metric_id"),
#         "inner"
#     )
# )

# # =================================================
# # AGGREGATE SCORE (FIX CỨNG THEO METRIC_CODE)
# # =================================================
# score_df = (
#     df.groupBy("f.calculating_at")
#     .agg(
#         # -------------------------------
#         # TOTAL SCORE
#         # -------------------------------
#         F.sum(F.col("f.metric_value") * F.col("m.metric_weight"))
#         .alias("market_score"),

#         # -------------------------------
#         # TREND (GUARD – KHÔNG DÙNG BUY)
#         # -------------------------------
#         F.sum(
#             F.when(
#                 F.col("m.metric_code").like("ETH_TREND_%"),
#                 F.col("f.metric_value") * F.col("m.metric_weight")
#             ).otherwise(0)
#         ).alias("metric_trend"),

#         # -------------------------------
#         # MOMENTUM (MACD)
#         # -------------------------------
#         F.sum(
#             F.when(
#                 F.col("m.metric_code").like("%MACD%"),
#                 F.col("f.metric_value") * F.col("m.metric_weight")
#             ).otherwise(0)
#         ).alias("metric_momentum"),

#         # -------------------------------
#         # VOLUME
#         # -------------------------------
#         F.sum(
#             F.when(
#                 F.col("m.metric_code").like("%VOLUME%"),
#                 F.col("f.metric_value") * F.col("m.metric_weight")
#             ).otherwise(0)
#         ).alias("metric_volume"),

#         # -------------------------------
#         # VOLATILITY
#         # -------------------------------
#         F.sum(
#             F.when(
#                 F.col("m.metric_code").like("%VOLATILITY%") |
#                 F.col("m.metric_code").like("%SQUEEZE%") |
#                 F.col("m.metric_code").like("%BREAKDOWN%"),
#                 F.col("f.metric_value") * F.col("m.metric_weight")
#             ).otherwise(0)
#         ).alias("metric_volatility"),
#     )
# )

# # =================================================
# # SAFETY – NO NULL
# # =================================================
# score_df = score_df.select(
#     F.col("calculating_at").alias("predicting_at"),
#     F.coalesce("market_score", F.lit(0)).alias("market_score"),
#     F.coalesce("metric_trend", F.lit(0)).alias("metric_trend"),
#     F.coalesce("metric_momentum", F.lit(0)).alias("metric_momentum"),
#     F.coalesce("metric_volume", F.lit(0)).alias("metric_volume"),
#     F.coalesce("metric_volatility", F.lit(0)).alias("metric_volatility"),
# )

# # =================================================
# # ACTION + CONFIDENCE
# # =================================================
# pred_df = (
#     score_df
#     .withColumn(
#         "confidence_score",
#         (F.col("market_score") / F.lit(MAX_SCORE))
#     )
#     .withColumn(
#         "action",
#         F.when(F.col("market_score") >= 5.0, "BUY")
#          .when(F.col("market_score") >= 3.0, "SIDEWAY")
#          .otherwise("SELL")
#     )
# )

# # =================================================
# # BUY_SOFT_V3 – PERCENTILE GUARD (🔥 QUAN TRỌNG)
# # =================================================
# pred_df = (
#     pred_df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") == "BUY") & (
#                 (F.col("metric_momentum") < F.lit(P60_MOMENTUM)) |
#                 (F.col("metric_volatility") > F.lit(P40_VOL)) |
#                 (F.col("confidence_score") < F.lit(P55_CONF))
#             ),
#             1
#         ).otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("metric_no_trade") == 1, "NOT_BUY_SOFT_PCTL")
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
#     pred_df
#     .join(price_df, "predicting_at", "left")
#     .select(
#         F.lit(SYMBOL_ID).alias("symbol_id"),
#         F.lit(INTERVAL_ID).alias("interval_id"),
#         "predicting_at",
#         "close",
#         "action",
#         F.round("market_score", 4).alias("market_score"),
#         F.round("confidence_score", 4).alias("confidence_score"),
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
#     .mode("append")   # backtest → truncate trước
#     .jdbc(url, "fact_prediction_1", properties=jdbc_props)
# )

# print("✅ BUY_SOFT_V3 PREDICTION ETH 2H DONE")
# spark.stop()

"""------------------------------------------------------------"""

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

# SYMBOL_ID = 2        # ETH
# INTERVAL_ID = 1      # 2H

# # percentile-based thresholds (derive từ data)
# P_MOMENTUM_MAX = 0.8     # momentum yếu
# P_TREND_MAX    = 3.5
# P_VOL_MAX      = 0.7
# CONF_MIN = 0.35
# CONF_MAX = 0.65

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_SELL_SOFT_V1_ETH_2H")
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

# # =================================================
# # JOIN METRIC
# # =================================================
# df = (
#     metric_df.alias("f")
#     .join(metric_dim.alias("m"), "metric_id", "inner")
# )

# # =================================================
# # AGG SCORE
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         # total score
#         F.sum(F.col("metric_value") * F.col("metric_weight"))
#             .alias("market_score"),

#         # trend
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_TREND_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_trend"),

#         # momentum
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_SELL_MACD_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_momentum"),

#         # volume
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_SELL_VOLUME_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_volume"),

#         # volatility
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_SELL_VOLATILITY_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_volatility"),
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
# # JOIN PRICE
# # =================================================
# pred = (
#     score_df
#     .withColumnRenamed("calculating_at", "predicting_at")
#     .join(price_df, "predicting_at", "left")
# )

# # =================================================
# # ACTION + NO-TRADE LOGIC
# # =================================================
# pred = (
#     pred
#     .withColumn(
#         "action",
#         F.when(
#             (F.col("metric_momentum") <= P_MOMENTUM_MAX) &
#             (F.col("metric_trend") <= P_TREND_MAX) &
#             (F.col("metric_volatility") <= P_VOL_MAX),
#             "SELL"
#         ).otherwise("SIDEWAY")
#     )
#     .withColumn(
#         "confidence_score",
#         F.round(F.col("market_score") / F.lit(10.0), 4)
#     )
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("confidence_score") < CONF_MIN) |
#             (F.col("confidence_score") > CONF_MAX),
#             1
#         ).otherwise(0)
#     )
#     .withColumn(
#         "reason_no_trade",
#         F.when(F.col("confidence_score") < CONF_MIN, "LOW_CONFIDENCE")
#          .when(F.col("confidence_score") > CONF_MAX, "OVER_CONFIDENCE")
#     )
# )

# # =================================================
# # FINAL SELECT
# # =================================================
# final_df = pred.select(
#     F.lit(SYMBOL_ID).alias("symbol_id"),
#     F.lit(INTERVAL_ID).alias("interval_id"),
#     "predicting_at",
#     "close",
#     "action",
#     F.round("market_score", 4).alias("market_score"),
#     "confidence_score",
#     F.round("metric_trend", 4).alias("metric_trend"),
#     F.round("metric_momentum", 4).alias("metric_momentum"),
#     F.round("metric_volume", 4).alias("metric_volume"),
#     F.round("metric_volatility", 4).alias("metric_volatility"),
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

# print("✅ PREDICTION SELL_SOFT_V1 ETH 2H DONE")
# spark.stop()

"""-------------------------------------------------------------"""




"""--------------------------------------------------------"""

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

# SYMBOL_ID = 2        # ETH
# INTERVAL_ID = 1      # 2H

# # ===== Thresholds (đã tune từ data bạn gửi) =====
# BUY_THRESHOLD  = 2.5
# SELL_THRESHOLD = 2.0
# MAX_SCORE = 5.0

# CONF_MIN = 0.65
# CONF_MAX = 0.85

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("Prediction_V5_ETH_2H_BUY_SELL_SEPARATE")
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
# # AGGREGATE BUY / SELL SCORE (TÁCH CHUẨN)
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         # BUY score (mean reversion / pullback)
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_BUY_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("buy_score"),

#         # SELL score (breakdown / weakness)
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_SELL_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("sell_score"),

#         # Trend context (KHÔNG tạo lệnh)
#         F.sum(
#             F.when(
#                 F.col("metric_code").like("ETH_TREND_%"),
#                 F.col("metric_value") * F.col("metric_weight")
#             ).otherwise(0)
#         ).alias("metric_trend")
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
# # CONFLICT DETECTION (CỰC QUAN TRỌNG)
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
# # ACTION LOGIC (KHÔNG SO BUY VS SELL)
# # =================================================
# df = df.withColumn(
#     "action",
#     F.when(F.col("metric_conflict") == 1, "SIDEWAY")
#      .when(F.col("buy_score")  >= BUY_THRESHOLD,  "BUY")
#      .when(F.col("sell_score") >= SELL_THRESHOLD, "SELL")
#      .otherwise("SIDEWAY")
# )

# # =================================================
# # CONFIDENCE SCORE (ĐỘC LẬP)
# # =================================================
# df = df.withColumn(
#     "confidence_score",
#     F.round(
#         F.when(F.col("action") == "BUY",  F.col("buy_score")  / F.lit(MAX_SCORE))
#          .when(F.col("action") == "SELL", F.col("sell_score") / F.lit(MAX_SCORE))
#          .otherwise(F.lit(0)),
#         4
#     )
# )

# # =================================================
# # NO-TRADE GUARD
# # =================================================
# df = (
#     df
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
#         F.when(F.col("metric_conflict") == 1, "CONFLICT")
#          .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
#          .when(F.col("confidence_score") > CONF_MAX, "OVER_CONF")
#          .when(F.col("action") == "SIDEWAY", "NO_EDGE")
#     )
# )

# # =================================================
# # FINAL SELECT (fact_prediction_1 schema)
# # =================================================
# final_df = df.select(
#     F.lit(SYMBOL_ID).alias("symbol_id"),
#     F.lit(INTERVAL_ID).alias("interval_id"),
#     "predicting_at",
#     "close",
#     "action",

#     # market_score = độ mạnh của phía được chọn
#     F.round(
#         F.when(F.col("action") == "BUY",  F.col("buy_score"))
#          .when(F.col("action") == "SELL", F.col("sell_score"))
#          .otherwise(F.lit(0)),
#         4
#     ).alias("market_score"),

#     "confidence_score",

#     F.round("metric_trend", 4).alias("metric_trend"),
#     F.round("buy_score",  4).alias("metric_momentum"),
#     F.round("sell_score", 4).alias("metric_volatility"),

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
#     .mode("append")   # backtest → truncate trước
#     .jdbc(url, "fact_prediction_1", properties=jdbc_props)
# )

# print("✅ PREDICTION V5 BUY / SELL SEPARATE DONE")
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

# SYMBOL_ID = 2        # ETH
# INTERVAL_ID = 1      # 2H

# # ===== SCORE =====
# BUY_THRESHOLD  = 2.2
# SELL_THRESHOLD = 2.0
# MAX_SCORE = 5.0

# # ===== ETH EDGE / CONF =====
# EDGE_MIN = 0.8
# CONF_MIN = 0.55
# CONF_MAX = 0.90

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("PREDICTION_ETH_2H_V6_FIXED")
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
# # AGG SCORE
# # =================================================
# score_df = (
#     df.groupBy("calculating_at")
#     .agg(
#         F.sum(F.when(F.col("metric_code").like("ETH_BUY_%"),
#                      F.col("metric_value") * F.col("metric_weight"))
#               .otherwise(0)).alias("buy_score"),

#         F.sum(F.when(F.col("metric_code").like("ETH_SELL_%"),
#                      F.col("metric_value") * F.col("metric_weight"))
#               .otherwise(0)).alias("sell_score"),
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
# # ACTION + EDGE
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
# # CONFLICT
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
# # ETH NO-TRADE (FIXED)
# # =================================================
# df = (
#     df
#     .withColumn(
#         "metric_no_trade",
#         F.when(
#             (F.col("action") == "SIDEWAY") |
#             (F.col("metric_conflict") == 1) |
#             (F.col("edge") < EDGE_MIN) |
#             (F.col("confidence_score") < CONF_MIN) |
#             (F.col("confidence_score") > CONF_MAX),
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
# # FINAL
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

# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction", properties=jdbc_props)
# )

# print("✅ PREDICTION_ETH_V6_FIXED DONE")
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

SYMBOL_ID   = 2   # ETH
INTERVAL_ID = 1   # 2H

# ===== SCORE =====
BUY_THRESHOLD  = 2.2
SELL_THRESHOLD = 2.0
MAX_SCORE      = 5.0

# ===== ETH EDGE / CONF =====
EDGE_MIN = 0.8
CONF_MIN = 0.55
CONF_MAX = 0.90

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("PREDICTION_ETH_2H_V6_SAFE")
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
                F.col("metric_code").like("ETH_BUY_%"),
                F.col("metric_value") * F.col("metric_weight")
            ).otherwise(0)
        ).alias("buy_score"),

        F.sum(
            F.when(
                F.col("metric_code").like("ETH_SELL_%"),
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
# ACTION + EDGE
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
# CONFLICT
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
# ETH NO-TRADE (GIỮ NGUYÊN RULE)
# =================================================
df = (
    df
    .withColumn(
        "metric_no_trade",
        F.when(
            (F.col("action") == "SIDEWAY") |
            (F.col("metric_conflict") == 1) |
            (F.col("edge") < EDGE_MIN) |
            (F.col("confidence_score") < CONF_MIN) |
            (F.col("confidence_score") > CONF_MAX),
            1
        ).otherwise(0)
    )
    .withColumn(
        "reason_no_trade",
        F.when(F.col("action") == "SIDEWAY", "NO_EDGE")
         .when(F.col("edge") < EDGE_MIN, "WEAK_EDGE")
         .when(F.col("metric_conflict") == 1, "CONFLICT")
         .when(F.col("confidence_score") < CONF_MIN, "LOW_CONF")
         .when(F.col("confidence_score") > CONF_MAX, "OVER_CONF")
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
    print("ℹ️ No new ETH prediction rows – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_prediction", properties=jdbc_props)
    )
    print("✅ ETH prediction inserted safely")

spark.stop()
