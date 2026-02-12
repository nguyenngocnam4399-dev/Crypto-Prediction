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

SYMBOL_ID = 3
INTERVAL_ID = 1

# ===== BUY =====
BUY_TP_EARLY  = 0.0015   # +0.15%
BUY_TP_RUN   = 0.0045   # +0.45%
BUY_SL       = 0.0030

# ===== SELL =====
SELL_TP_WEAK   = 0.0025
SELL_TP_STRONG = 0.0060
SELL_SL        = 0.0035
SELL_STRONG_EDGE = 3.0

LOOKAHEAD = 4

# =================================================
spark = (
    SparkSession.builder
    .appName("CONFIRM_BNB_WINRATE_PNL_BALANCE")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =================================================
url = f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}?useSSL=false"
jdbc_props = {
    "user": DB["user"],
    "password": DB["password"],
    "driver": "com.mysql.cj.jdbc.Driver",
    "rewriteBatchedStatements": "true"
}

prediction = spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
confirmed  = spark.read.jdbc(url, "fact_prediction_result", properties=jdbc_props)

# =================================================
# LOAD PRED (SAFE)
# =================================================
pred = (
    prediction.alias("p")
    .join(
        confirmed.select("prediction_id").alias("r"),
        F.col("p.id") == F.col("r.prediction_id"),
        "left_anti"
    )
    .filter(
        (F.col("p.symbol_id") == SYMBOL_ID) &
        (F.col("p.interval_id") == INTERVAL_ID) &
        (F.col("p.metric_no_trade") == 0) &
        (F.col("p.action").isin("BUY", "SELL"))
    )
    .select(
        F.col("p.id").alias("prediction_id"),
        "p.action",
        "p.predicting_at",
        F.col("p.close").alias("entry_price"),
        F.col("p.market_score").alias("edge")
    )
    .withColumn(
        "sell_is_strong",
        (F.col("action") == "SELL") & (F.col("edge") >= SELL_STRONG_EDGE)
    )
)

# =================================================
# LOAD KLINE
# =================================================
kline = (
    spark.read.jdbc(url, "fact_kline", properties=jdbc_props)
    .filter(
        (F.col("symbol_id") == SYMBOL_ID) &
        (F.col("interval_id") == INTERVAL_ID)
    )
    .select(
        F.col("close_time").cast("timestamp").alias("k_time"),
        "high_price",
        "low_price"
    )
)

joined = (
    pred.alias("p")
    .join(
        kline.alias("k"),
        (F.col("k.k_time") > F.col("p.predicting_at")) &
        (F.col("k.k_time") <= F.expr(f"p.predicting_at + INTERVAL {LOOKAHEAD} HOURS")),
        "inner"
    )
)

agg = (
    joined
    .groupBy("prediction_id", "action", "entry_price", "sell_is_strong")
    .agg(
        F.max((F.col("high_price") - F.col("entry_price")) / F.col("entry_price")).alias("max_ret"),
        F.min((F.col("low_price")  - F.col("entry_price")) / F.col("entry_price")).alias("min_ret"),
        F.max("k_time").alias("confirmed_at")
    )
)

# =================================================
# RESULT LOGIC — BALANCED
# =================================================
result = (
    agg
    .withColumn(
        "exit_pct",
        F.when(
            (F.col("action") == "BUY") & (F.col("max_ret") >= BUY_TP_RUN), BUY_TP_RUN
        ).when(
            (F.col("action") == "BUY") & (F.col("max_ret") >= BUY_TP_EARLY), BUY_TP_EARLY
        ).when(
            (F.col("action") == "BUY") & (F.col("min_ret") <= -BUY_SL), -BUY_SL
        ).when(
            (F.col("action") == "SELL") & (F.col("sell_is_strong")) & (F.col("min_ret") <= -SELL_TP_STRONG),
            SELL_TP_STRONG
        ).when(
            (F.col("action") == "SELL") & (F.col("min_ret") <= -SELL_TP_WEAK),
            SELL_TP_WEAK
        ).otherwise(-SELL_SL)
    )
    .withColumn(
        "exit_price",
        F.when(
            F.col("action") == "BUY",
            F.col("entry_price") * (1 + F.col("exit_pct"))
        ).otherwise(
            F.col("entry_price") * (1 - F.col("exit_pct"))
        )
    )
    .withColumn(
        "pnl_pct",
        F.round(
            F.when(
                F.col("action") == "BUY",
                (F.col("exit_price") - F.col("entry_price")) / F.col("entry_price")
            ).otherwise(
                (F.col("entry_price") - F.col("exit_price")) / F.col("entry_price")
            ),
            6
        )
    )
    .withColumn(
        "result_status",
        F.when(F.col("pnl_pct") > 0, "WIN").otherwise("LOSS")
    )
)

final_df = result.select(
    "prediction_id",
    "result_status",
    "pnl_pct",
    "exit_price",
    "confirmed_at"
)

final_new = final_df.join(
    confirmed.select("prediction_id"),
    "prediction_id",
    "left_anti"
)

if not final_new.rdd.isEmpty():
    final_new.write.mode("append").jdbc(url, "fact_prediction_result", properties=jdbc_props)

print("✅ CONFIRM_BNB_WINRATE_PNL_BALANCE DONE")
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

# SYMBOL_ID = 3
# INTERVAL_ID = 1

# TP_PCT = 0.0025
# SL_PCT = 0.0025
# LOOKAHEAD_HOURS = 4

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_BNB_2H_FIXED_SAFE")
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
# # LOAD TABLES
# # =================================================
# prediction = spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
# confirmed  = spark.read.jdbc(url, "fact_prediction_result", properties=jdbc_props)

# # =================================================
# # LOAD PREDICTION (CHƯA CONFIRM)
# # =================================================
# pred = (
#     prediction.alias("p")
#     .join(
#         confirmed.select("prediction_id").alias("r"),
#         F.col("p.id") == F.col("r.prediction_id"),
#         "left_anti"
#     )
#     .filter(
#         (F.col("p.symbol_id") == SYMBOL_ID) &
#         (F.col("p.interval_id") == INTERVAL_ID) &
#         (F.col("p.action").isin("BUY", "SELL"))
#     )
#     .select(
#         F.col("p.id").alias("prediction_id"),
#         "p.action",
#         "p.predicting_at",
#         F.col("p.close").alias("entry_price")
#     )
# )

# # =================================================
# # LOAD KLINE
# # =================================================
# kline = (
#     spark.read.jdbc(url, "fact_kline", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
#     .select(
#         F.col("close_time").cast("timestamp").alias("k_time"),
#         "high_price",
#         "low_price"
#     )
# )

# # =================================================
# # JOIN FUTURE WINDOW
# # =================================================
# joined = (
#     pred.alias("p")
#     .join(
#         kline.alias("k"),
#         (F.col("k.k_time") > F.col("p.predicting_at")) &
#         (F.col("k.k_time") <= F.expr(f"p.predicting_at + INTERVAL {LOOKAHEAD_HOURS} HOURS")),
#         "inner"
#     )
# )

# # =================================================
# # AGG FUTURE MOVE
# # =================================================
# agg = (
#     joined
#     .groupBy("prediction_id", "action", "entry_price")
#     .agg(
#         F.max((F.col("high_price") - F.col("entry_price")) / F.col("entry_price")).alias("max_ret"),
#         F.min((F.col("low_price")  - F.col("entry_price")) / F.col("entry_price")).alias("min_ret"),
#         F.max("k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # CONFIRM LOGIC (FIXED TP / SL)
# # =================================================
# result = (
#     agg
#     .withColumn(
#         "result_status",
#         F.when(
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_PCT), "WIN"
#         ).when(
#             (F.col("action") == "BUY") & (F.col("min_ret") <= -SL_PCT), "LOSS"
#         ).when(
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_PCT), "WIN"
#         ).when(
#             (F.col("action") == "SELL") & (F.col("max_ret") >= SL_PCT), "LOSS"
#         ).otherwise("LOSS")
#     )
#     .withColumn(
#         "pnl_pct",
#         F.when(F.col("result_status") == "WIN",  TP_PCT)
#          .otherwise(-SL_PCT)
#     )
# )

# final_df = result.select(
#     "prediction_id",
#     "result_status",
#     F.round("pnl_pct", 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# # =================================================
# # SAFE WRITE
# # =================================================
# final_new = (
#     final_df
#     .join(
#         confirmed.select("prediction_id"),
#         "prediction_id",
#         "left_anti"
#     )
# )

# if final_new.rdd.isEmpty():
#     print("ℹ️ No new confirms – safe skip")
# else:
#     (
#         final_new
#         .write
#         .mode("append")
#         .jdbc(url, "fact_prediction_result", properties=jdbc_props)
#     )
#     print("✅ CONFIRM BNB 2H SAFE DONE")

# spark.stop()


"""--------------------2106	0.011767	0.8514-------------"""
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

# SYMBOL_ID = 3      # BNB
# INTERVAL_ID = 1    # 2H

# # TP / SL cho 2H
# TP_PCT = 0.0025    # 0.25%
# SL_PCT = 0.0025    # 0.25%

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_BUY_SELL_2H")
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
# # LOAD PREDICTION (BUY + SELL)
# # =================================================
# pred = (
#     spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID) &
#         (F.col("action").isin("BUY", "SELL"))
#     )
# )

# # =================================================
# # LOAD KLINE
# # =================================================
# kline = (
#     spark.read.jdbc(url, "fact_kline", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
#     .select(
#         F.col("close_time").cast("timestamp").alias("k_time"),
#         "high_price",
#         "low_price"
#     )
# )

# # =================================================
# # JOIN FUTURE 2 CANDLES (4H)
# # =================================================
# joined = (
#     pred.alias("p")
#     .join(
#         kline.alias("k"),
#         (F.col("k.k_time") > F.col("p.predicting_at")) &
#         (F.col("k.k_time") <= F.expr("p.predicting_at + INTERVAL 4 HOURS")),
#         "inner"
#     )
# )

# # =================================================
# # CALC RETURN
# # =================================================
# ret_df = (
#     joined
#     .groupBy("p.id", "p.action", "p.close")
#     .agg(
#         F.max((F.col("k.high_price") - F.col("p.close")) / F.col("p.close")).alias("max_ret"),
#         F.min((F.col("k.low_price") - F.col("p.close")) / F.col("p.close")).alias("min_ret"),
#         F.max("k.k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # CONFIRM LOGIC (BUY vs SELL)
# # =================================================
# result = (
#     ret_df
#     .withColumn(
#         "result_status",
#         F.when(
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_PCT),
#             "WIN"
#         )
#         .when(
#             (F.col("action") == "BUY") & (F.col("min_ret") <= -SL_PCT),
#             "LOSS"
#         )
#         .when(
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_PCT),
#             "WIN"
#         )
#         .when(
#             (F.col("action") == "SELL") & (F.col("max_ret") >= SL_PCT),
#             "LOSS"
#         )
#         .otherwise("LOSS")
#     )
#     .withColumn(
#         "pnl_pct",
#         F.when(F.col("action") == "BUY", F.col("max_ret"))
#          .otherwise(-F.col("min_ret"))
#     )
# )

# # =================================================
# # FINAL
# # =================================================
# final_df = result.select(
#     F.col("id").alias("prediction_id"),
#     "result_status",
#     F.round(F.col("pnl_pct"), 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# # =================================================
# # WRITE RESULT
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction_result", properties=jdbc_props)
# )

# print("✅ CONFIRM BUY + SELL 2H DONE")
# spark.stop()

"""-----------------------------------------------------------------"""




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

# SYMBOL_ID = 3
# INTERVAL_ID = 1

# # ===== RR (FINAL) =====
# TP_PCT = 0.0035   # 0.35%
# SL_PCT = 0.0025   # 0.25%

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_BNB_2H_EDGE6_FINAL")
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
# # LOAD PREDICTION (ONLY VALID TRADE)
# # =================================================
# pred = (
#     spark.read.jdbc(url, "fact_prediction_1", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID) &
#         (F.col("metric_no_trade") == 0) &
#         (F.col("action").isin("BUY", "SELL"))
#     )
# )

# # =================================================
# # LOAD KLINE
# # =================================================
# kline = (
#     spark.read.jdbc(url, "fact_kline", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID)
#     )
#     .select(
#         F.col("close_time").cast("timestamp").alias("k_time"),
#         "high_price",
#         "low_price"
#     )
# )

# # =================================================
# # JOIN FUTURE 4H
# # =================================================
# joined = (
#     pred.alias("p")
#     .join(
#         kline.alias("k"),
#         (F.col("k.k_time") > F.col("p.predicting_at")) &
#         (F.col("k.k_time") <= F.expr("p.predicting_at + INTERVAL 4 HOURS")),
#         "inner"
#     )
# )

# # =================================================
# # RETURN
# # =================================================
# ret_df = (
#     joined
#     .groupBy("p.id", "p.action", "p.close")
#     .agg(
#         F.max((F.col("k.high_price") - F.col("p.close")) / F.col("p.close")).alias("max_ret"),
#         F.min((F.col("k.low_price") - F.col("p.close")) / F.col("p.close")).alias("min_ret"),
#         F.max("k.k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # CONFIRM
# # =================================================
# result = (
#     ret_df
#     .withColumn(
#         "result_status",
#         F.when(
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_PCT),
#             "WIN"
#         )
#         .when(
#             (F.col("action") == "BUY") & (F.col("min_ret") <= -SL_PCT),
#             "LOSS"
#         )
#         .when(
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_PCT),
#             "WIN"
#         )
#         .when(
#             (F.col("action") == "SELL") & (F.col("max_ret") >= SL_PCT),
#             "LOSS"
#         )
#         .otherwise("LOSS")
#     )
#     .withColumn(
#         "pnl_pct",
#         F.when(F.col("action") == "BUY", F.col("max_ret"))
#          .otherwise(-F.col("min_ret"))
#     )
# )

# # =================================================
# # WRITE RESULT
# # =================================================
# (
#     result.select(
#         F.col("id").alias("prediction_id"),
#         "result_status",
#         F.round("pnl_pct", 6).alias("pnl_pct"),
#         "confirmed_at"
#     )
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction_result_1", properties=jdbc_props)
# )

# print("✅ CONFIRM BUY + SELL 2H DONE — EDGE ≥ 6.0 FINAL")
# spark.stop()
