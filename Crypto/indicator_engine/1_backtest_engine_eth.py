# from pyspark.sql import SparkSession, functions as F, Window

# # =================================================
# # DB CONFIG
# # =================================================
# DB = {
#     "host": "localhost",
#     "port": "3306",
#     "db": "crypto_dw",
#     "user": "spark_user",
#     "password": "spark123"
# }

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_BUY_SOFT_V3_ETH_2H")
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
# # PARAM
# # =================================================
# SYMBOL_ID = 2        # ETH
# INTERVAL_ID = 1     # 2H

# TP_PCT = 0.002
# SL_PCT = 0.002
# LOOKAHEAD_HOURS = 4

# # =================================================
# # LOAD PREDICTION (BUY_SOFT_V3 only)
# # =================================================
# pred = (
#     spark.read.jdbc(url, "fact_prediction_1", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID) &
#         (F.col("action") == "BUY") &
#         (F.col("metric_no_trade") == 0)
#     )
#     .select(
#         "id",
#         "predicting_at",
#         F.col("close").alias("entry_price")
#     )
# )

# print("BUY_SOFT_V3 trades:", pred.count())

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
#         F.col("close_time").cast("timestamp").alias("close_time"),
#         "high_price",
#         "low_price"
#     )
# )

# # =================================================
# # JOIN → FUTURE 2 CANDLES (4H)
# # =================================================
# joined = (
#     pred.alias("p")
#     .join(
#         kline.alias("k"),
#         (
#             (F.col("k.close_time") > F.col("p.predicting_at")) &
#             (F.col("k.close_time") <=
#              F.expr(f"p.predicting_at + INTERVAL {LOOKAHEAD_HOURS} HOURS"))
#         ),
#         "inner"
#     )
# )

# # =================================================
# # AGG FUTURE MOVE
# # =================================================
# confirm = (
#     joined
#     .groupBy("p.id", "p.entry_price", "p.predicting_at")
#     .agg(
#         F.max(
#             (F.col("k.high_price") - F.col("p.entry_price")) /
#             F.col("p.entry_price")
#         ).alias("max_ret"),

#         F.min(
#             (F.col("k.low_price") - F.col("p.entry_price")) /
#             F.col("p.entry_price")
#         ).alias("min_ret"),

#         F.max("k.close_time").alias("confirmed_at")
#     )
# )

# print("Confirm rows:", confirm.count())

# # =================================================
# # RESULT LOGIC (BUY)
# # =================================================
# result = (
#     confirm
#     .withColumn(
#         "result_status",
#         F.when(F.col("max_ret") >= TP_PCT, "WIN")
#          .when(F.col("min_ret") <= -SL_PCT, "LOSS")
#          .otherwise("LOSS")
#     )
#     .withColumn(
#         "pnl_pct",
#         F.when(F.col("max_ret") >= TP_PCT, TP_PCT)
#          .when(F.col("min_ret") <= -SL_PCT, -SL_PCT)
#          .otherwise(0)
#     )
# )

# # =================================================
# # FINAL DF
# # =================================================
# final_df = result.select(
#     F.col("id").alias("prediction_id"),
#     "result_status",
#     F.round("pnl_pct", 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# print("Final confirm:", final_df.count())

# # =================================================
# # WRITE RESULT
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(
#         url=url,
#         table="fact_prediction_result_1",
#         properties=jdbc_props
#     )
# )

# print("✅ CONFIRM BUY_SOFT_V3 ETH 2H DONE")
# spark.stop()


"""---------------------------------------------------------------"""


# from pyspark.sql import SparkSession, functions as F, Window

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

# TP_PCT = 0.0020      # TAKE PROFIT (down)
# SL_PCT = 0.0020      # STOP LOSS (up)
# LOOKAHEAD_HOURS = 4  # 2 candles ahead

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_SELL_ETH_2H")
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
#     "driver": "com.mysql.cj.jdbc.Driver",
#     "rewriteBatchedStatements": "true"
# }

# # =================================================
# # LOAD PREDICTION (SELL only, tradable)
# # =================================================
# pred = (
#     spark.read.jdbc(url, "fact_prediction_1", properties=jdbc_props)
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID) &
#         (F.col("action") == "SELL") &
#         (F.col("metric_no_trade") == 0)
#     )
# )

# print("SELL predictions:", pred.count())

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
# # JOIN → NEXT CANDLES
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

# print("Joined rows:", joined.count())

# # =================================================
# # CONFIRM LOGIC (SELL)
# # =================================================
# confirm = (
#     joined
#     .groupBy("p.id", "p.predicting_at", "p.close")
#     .agg(
#         # Giá giảm → lời
#         F.min(
#             (F.col("k.low_price") - F.col("p.close")) / F.col("p.close")
#         ).alias("min_ret"),

#         # Giá tăng → lỗ
#         F.max(
#             (F.col("k.high_price") - F.col("p.close")) / F.col("p.close")
#         ).alias("max_ret"),

#         F.max("k.k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # RESULT
# # =================================================
# result = confirm.withColumn(
#     "result_status",
#     F.when(F.col("min_ret") <= -TP_PCT, "WIN")
#      .when(F.col("max_ret") >= SL_PCT, "LOSS")
#      .otherwise("LOSS")
# )

# # =================================================
# # FINAL OUTPUT
# # =================================================
# final_df = result.select(
#     F.col("id").alias("prediction_id"),
#     "result_status",
#     F.round(F.col("min_ret"), 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# print("Final SELL confirms:", final_df.count())

# # =================================================
# # WRITE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(
#         url=url,
#         table="fact_prediction_result_1",
#         properties=jdbc_props
#     )
# )

# print("✅ CONFIRM SELL ETH 2H DONE")
# spark.stop()



"""------------------------0.004745-----------------------------------"""


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

# # ===== EXIT PARAM =====
# TP1_PCT = 0.0025     # +0.25%
# TP2_LOW = 0.0060     # +0.60%
# TP2_HIGH = 0.0080    # +0.80%
# SL_PCT  = 0.0025     # -0.25%

# LOOKAHEAD_HOURS = 6

# CONF_STRONG = 0.75   # adaptive TP2 threshold

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_V3_FINAL_ETH_2H")
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
#     "driver": "com.mysql.cj.jdbc.Driver",
#     "rewriteBatchedStatements": "true"
# }

# # =================================================
# # LOAD PREDICTION (CHƯA CONFIRM)
# # =================================================
# pred = (
#     spark.read.jdbc(url, "fact_prediction_1", properties=jdbc_props).alias("p")
#     .join(
#         spark.read.jdbc(url, "fact_prediction_result_1", properties=jdbc_props)
#         .select("prediction_id").alias("r"),
#         F.col("p.id") == F.col("r.prediction_id"),
#         "left_anti"
#     )
#     .filter(
#         (F.col("symbol_id") == SYMBOL_ID) &
#         (F.col("interval_id") == INTERVAL_ID) &
#         (F.col("metric_no_trade") == 0) &
#         (F.col("action").isin("BUY", "SELL"))
#     )
#     .select(
#         F.col("p.id"),
#         "action",
#         "predicting_at",
#         F.col("close").alias("entry_price"),
#         "confidence_score"
#     )
# )

# print("Predictions to confirm:", pred.count())

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
# # JOIN FUTURE CANDLES
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
#     .groupBy("p.id", "p.action", "p.entry_price", "p.predicting_at", "p.confidence_score")
#     .agg(
#         F.max((F.col("k.high_price") - F.col("p.entry_price")) / F.col("p.entry_price")).alias("max_ret"),
#         F.min((F.col("k.low_price")  - F.col("p.entry_price")) / F.col("p.entry_price")).alias("min_ret"),
#         F.max("k.k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # RESULT LOGIC — TP2 ADAPTIVE
# # =================================================
# result = (
#     agg
#     .withColumn(
#         "tp2_used",
#         F.when(F.col("confidence_score") >= CONF_STRONG, TP2_HIGH)
#          .otherwise(TP2_LOW)
#     )
#     .withColumn(
#         "pnl_pct",
#         F.when(
#             (F.col("action") == "BUY") &
#             (F.col("max_ret") >= F.col("tp2_used")),
#             F.col("tp2_used")
#         ).when(
#             (F.col("action") == "SELL") &
#             (F.col("min_ret") <= -F.col("tp2_used")),
#             F.col("tp2_used")
#         ).when(
#             (F.col("action") == "BUY") &
#             (F.col("max_ret") >= TP1_PCT),
#             TP1_PCT
#         ).when(
#             (F.col("action") == "SELL") &
#             (F.col("min_ret") <= -TP1_PCT),
#             TP1_PCT
#         ).otherwise(-SL_PCT)
#     )
#     .withColumn(
#         "result_status",
#         F.when(F.col("pnl_pct") > 0, "WIN").otherwise("LOSS")
#     )
# )

# # =================================================
# # FINAL OUTPUT
# # =================================================
# final_df = result.select(
#     F.col("id").alias("prediction_id"),
#     "result_status",
#     F.round("pnl_pct", 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# print("Final confirms:", final_df.count())

# # =================================================
# # WRITE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction_result_1", properties=jdbc_props)
# )

# print("✅ CONFIRM_V3_FINAL DONE")
# spark.stop()


"""------------------------------------------------------"""

"""------------------------------------------------------"""


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

# # ================= EDGE TIER =================
# EDGE_CUT = 3.7       # 🔥 CHỐT từ SQL: edge >= 3.7 mới sống

# # ================= EXIT PARAM =================
# TP_RUNNER = 0.0080   # +0.8%  (edge cao → để chạy)
# TP_BASE   = 0.0035   # +0.35% (fallback)
# SL_BASE   = 0.0030   # -0.3%

# LOOKAHEAD_HOURS = 6  # cho ETH đủ thời gian chạy

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_ETH_EDGE_TIER_FINAL")
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
#     "driver": "com.mysql.cj.jdbc.Driver",
#     "rewriteBatchedStatements": "true"
# }

# # =================================================
# # LOAD PREDICTION (CHƯA CONFIRM)
# # =================================================
# # - chỉ lấy prediction chưa có result
# # - không sửa logic prediction
# pred = (
#     spark.read.jdbc(url, "fact_prediction", properties=jdbc_props).alias("p")
#     .join(
#         spark.read.jdbc(url, "fact_prediction_result", properties=jdbc_props)
#         .select("prediction_id").alias("r"),
#         F.col("p.id") == F.col("r.prediction_id"),
#         "left_anti"
#     )
#     .filter(
#         (F.col("p.symbol_id") == SYMBOL_ID) &
#         (F.col("p.interval_id") == INTERVAL_ID) &
#         (F.col("p.metric_no_trade") == 0) &
#         (F.col("p.action").isin("BUY", "SELL")) &
#         (F.col("p.market_score") >= EDGE_CUT)     # 🔥 EDGE TIER GATE
#     )
#     .select(
#         F.col("p.id"),
#         "p.action",
#         "p.predicting_at",
#         F.col("p.close").alias("entry_price"),
#         F.col("p.market_score").alias("edge")
#     )
# )

# print("ETH predictions to confirm (edge >= 3.7):", pred.count())

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
# # JOIN FUTURE CANDLES
# # =================================================
# joined = (
#     pred.alias("p")
#     .join(
#         kline.alias("k"),
#         (F.col("k.k_time") > F.col("p.predicting_at")) &
#         (F.col("k.k_time") <=
#          F.expr(f"p.predicting_at + INTERVAL {LOOKAHEAD_HOURS} HOURS")),
#         "inner"
#     )
# )

# # =================================================
# # AGG FUTURE MOVE
# # =================================================
# agg = (
#     joined
#     .groupBy("p.id", "p.action", "p.entry_price")
#     .agg(
#         F.max(
#             (F.col("k.high_price") - F.col("p.entry_price"))
#             / F.col("p.entry_price")
#         ).alias("max_ret"),
#         F.min(
#             (F.col("k.low_price") - F.col("p.entry_price"))
#             / F.col("p.entry_price")
#         ).alias("min_ret"),
#         F.max("k.k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # RESULT LOGIC (EDGE SURVIVOR PAYOFF)
# # =================================================
# result = (
#     agg
#     .withColumn(
#         "pnl_pct",
#         F.when(
#             # BUY
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_RUNNER),
#             TP_RUNNER
#         ).when(
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_BASE),
#             TP_BASE
#         ).when(
#             (F.col("action") == "BUY") & (F.col("min_ret") <= -SL_BASE),
#             -SL_BASE
#         ).when(
#             # SELL
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_RUNNER),
#             TP_RUNNER
#         ).when(
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_BASE),
#             TP_BASE
#         ).otherwise(-SL_BASE)
#     )
#     .withColumn(
#         "result_status",
#         F.when(F.col("pnl_pct") > 0, "WIN").otherwise("LOSS")
#     )
# )

# # =================================================
# # FINAL OUTPUT
# # =================================================
# final_df = result.select(
#     F.col("id").alias("prediction_id"),
#     "result_status",
#     F.round("pnl_pct", 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# print("ETH confirmed trades:", final_df.count())

# # =================================================
# # WRITE
# # =================================================
# (
#     final_df
#     .write
#     .mode("append")
#     .jdbc(url, "fact_prediction_result", properties=jdbc_props)
# )

# print("✅ CONFIRM_ETH_EDGE_TIER_FINAL DONE")
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

# EDGE_CUT = 3.7

# TP_RUNNER = 0.0080
# TP_BASE   = 0.0035
# SL_BASE   = 0.0030

# LOOKAHEAD_HOURS = 6

# # =================================================
# # SPARK
# # =================================================
# spark = (
#     SparkSession.builder
#     .appName("CONFIRM_ETH_EDGE_TIER_FINAL_SAFE")
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
# # LOAD PREDICTION (CHƯA CONFIRM)
# # =================================================
# prediction = spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
# confirmed  = spark.read.jdbc(url, "fact_prediction_result", properties=jdbc_props)

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
#         (F.col("p.metric_no_trade") == 0) &
#         (F.col("p.action").isin("BUY", "SELL")) &
#         (F.col("p.market_score") >= EDGE_CUT)
#     )
#     .select(
#         F.col("p.id").alias("prediction_id"),
#         "p.action",
#         "p.predicting_at",
#         F.col("p.close").alias("entry_price"),
#         F.col("p.market_score").alias("edge")
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
#         (F.col("k.k_time") <=
#          F.expr(f"p.predicting_at + INTERVAL {LOOKAHEAD_HOURS} HOURS")),
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
#         F.max(
#             (F.col("k.high_price") - F.col("entry_price")) / F.col("entry_price")
#         ).alias("max_ret"),
#         F.min(
#             (F.col("k.low_price") - F.col("entry_price")) / F.col("entry_price")
#         ).alias("min_ret"),
#         F.max("k.k_time").alias("confirmed_at")
#     )
# )

# # =================================================
# # RESULT LOGIC (GIỮ NGUYÊN)
# # =================================================
# result = (
#     agg
#     .withColumn(
#         "pnl_pct",
#         F.when(
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_RUNNER), TP_RUNNER
#         ).when(
#             (F.col("action") == "BUY") & (F.col("max_ret") >= TP_BASE), TP_BASE
#         ).when(
#             (F.col("action") == "BUY") & (F.col("min_ret") <= -SL_BASE), -SL_BASE
#         ).when(
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_RUNNER), TP_RUNNER
#         ).when(
#             (F.col("action") == "SELL") & (F.col("min_ret") <= -TP_BASE), TP_BASE
#         ).otherwise(-SL_BASE)
#     )
#     .withColumn(
#         "result_status",
#         F.when(F.col("pnl_pct") > 0, "WIN").otherwise("LOSS")
#     )
# )

# final_df = result.select(
#     "prediction_id",
#     "result_status",
#     F.round("pnl_pct", 6).alias("pnl_pct"),
#     "confirmed_at"
# )

# # =================================================
# # IDEMPOTENT WRITE (ANTI-JOIN LẦN CUỐI)
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
#     print("ℹ️ No new ETH confirms – safe skip")
# else:
#     (
#         final_new
#         .write
#         .mode("append")
#         .jdbc(url, "fact_prediction_result", properties=jdbc_props)
#     )
#     print("✅ ETH confirm inserted safely")

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

SYMBOL_ID = 2        # ETH
INTERVAL_ID = 1      # 2H

# ===== BUY =====
BUY_TP = 0.0025
BUY_SL = 0.0020
BUY_LOOKAHEAD = 4

# ===== SELL =====
SELL_TP_WEAK   = 0.0030
SELL_TP_STRONG = 0.0060
SELL_SL_WEAK   = 0.0030
SELL_SL_STRONG = 0.0040

SELL_LOOKAHEAD_WEAK   = 6
SELL_LOOKAHEAD_STRONG = 8
SELL_STRONG_EDGE = 2.5

# =================================================
spark = (
    SparkSession.builder
    .appName("CONFIRM_ETH_EDGE_ADAPTIVE_FIXED_V2")
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

# =================================================
# LOAD TABLES
# =================================================
prediction = spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
confirmed  = spark.read.jdbc(url, "fact_prediction_result", properties=jdbc_props)

# =================================================
# LOAD PREDICTION (ETH tradable, chưa confirm)
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

# =================================================
# JOIN FUTURE CANDLES
# =================================================
joined = (
    pred.alias("p")
    .join(
        kline.alias("k"),
        (F.col("k.k_time") > F.col("p.predicting_at")) &
        (
            ((F.col("p.action") == "BUY") &
             (F.col("k.k_time") <= F.expr(f"p.predicting_at + INTERVAL {BUY_LOOKAHEAD} HOURS")))

            |

            ((F.col("p.sell_is_strong")) &
             (F.col("k.k_time") <= F.expr(f"p.predicting_at + INTERVAL {SELL_LOOKAHEAD_STRONG} HOURS")))

            |

            ((~F.col("p.sell_is_strong")) &
             (F.col("p.action") == "SELL") &
             (F.col("k.k_time") <= F.expr(f"p.predicting_at + INTERVAL {SELL_LOOKAHEAD_WEAK} HOURS")))
        ),
        "inner"
    )
)

# =================================================
# AGG FUTURE MOVE
# =================================================
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
# RESULT LOGIC (CHỈ CONFIRM KHI HIT TP / SL)
# =================================================
result = (
    agg
    .withColumn(
        "exit_pct",
        F.when(
            (F.col("action") == "BUY") & (F.col("max_ret") >= BUY_TP), BUY_TP
        ).when(
            (F.col("action") == "BUY") & (F.col("min_ret") <= -BUY_SL), -BUY_SL
        ).when(
            (F.col("action") == "SELL") & (F.col("sell_is_strong")) & (F.col("min_ret") <= -SELL_TP_STRONG),
            SELL_TP_STRONG
        ).when(
            (F.col("action") == "SELL") & (~F.col("sell_is_strong")) & (F.col("min_ret") <= -SELL_TP_WEAK),
            SELL_TP_WEAK
        ).when(
            (F.col("action") == "SELL") & (F.col("sell_is_strong")) & (F.col("max_ret") >= SELL_SL_STRONG),
            -SELL_SL_STRONG
        ).when(
            (F.col("action") == "SELL") & (~F.col("sell_is_strong")) & (F.col("max_ret") >= SELL_SL_WEAK),
            -SELL_SL_WEAK
        )
    )
    .filter(F.col("exit_pct").isNotNull())   # 🔥 không hit → bỏ
    .withColumn(
        "exit_price",
        F.when(
            F.col("action") == "BUY",
            F.col("entry_price") * (1 + F.col("exit_pct"))
        ).otherwise(
            F.col("entry_price") * (1 - F.abs(F.col("exit_pct")))
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

# =================================================
# FINAL + SAFE WRITE
# =================================================
final_new = (
    result.select(
        "prediction_id",
        "result_status",
        "pnl_pct",
        "exit_price",
        "confirmed_at"
    )
    .join(
        confirmed.select("prediction_id"),
        "prediction_id",
        "left_anti"
    )
)

if final_new.rdd.isEmpty():
    print("ℹ️ No new ETH confirms – safe skip")
else:
    final_new.write.mode("append").jdbc(
        url, "fact_prediction_result", properties=jdbc_props
    )
    print("✅ CONFIRM_ETH_EDGE_ADAPTIVE_FIXED_V2 DONE")

spark.stop()
