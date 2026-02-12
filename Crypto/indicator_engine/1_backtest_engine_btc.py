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

SYMBOL_ID = 1        # BTC
INTERVAL_ID = 1      # 2H

# ================= BUY =================
BUY_TP_BASE = 0.0025
BUY_TP_RUN  = 0.0080
BUY_SL      = 0.0020
BUY_LOOKAHEAD = 4

# ================= SELL =================
SELL_TP_WEAK   = 0.0030
SELL_TP_STRONG = 0.0120
SELL_SL_WEAK   = 0.0020
SELL_SL_STRONG = 0.0030

SELL_LOOKAHEAD_WEAK   = 6
SELL_LOOKAHEAD_STRONG = 10

BUY_RUN_EDGE     = 2.2
SELL_STRONG_EDGE = 2.5

# ================= LOW FREQ GUARD =================
EDGE_MIN = 2.8          # 🔥 giảm trade
CONF_MIN = 0.45         # 🔥 lọc tín hiệu yếu

# =================================================
# SPARK
# =================================================
spark = (
    SparkSession.builder
    .appName("CONFIRM_BTC_LOW_FREQ_DUAL_TP_FINAL_FIXED")
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
    "driver": "com.mysql.cj.jdbc.Driver",
    "rewriteBatchedStatements": "true"
}

# =================================================
# LOAD TABLES
# =================================================
prediction = spark.read.jdbc(url, "fact_prediction", properties=jdbc_props)
confirmed  = spark.read.jdbc(url, "fact_prediction_result", properties=jdbc_props)

# =================================================
# LOAD PREDICTION (CHƯA CONFIRM)
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
        (F.col("p.confidence_score") >= CONF_MIN) &
        (F.col("p.market_score") >= EDGE_MIN) &
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
        "buy_is_runner",
        (F.col("action") == "BUY") & (F.col("edge") >= BUY_RUN_EDGE)
    )
    .withColumn(
        "sell_is_strong",
        (F.col("action") == "SELL") & (F.col("edge") >= SELL_STRONG_EDGE)
    )
    .withColumn(
        "lookahead_h",
        F.when(F.col("action") == "BUY", BUY_LOOKAHEAD)
         .when(F.col("sell_is_strong"), SELL_LOOKAHEAD_STRONG)
         .otherwise(SELL_LOOKAHEAD_WEAK)
    )
)

print("BTC predictions to confirm:", pred.count())

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
# JOIN FUTURE WINDOW (🔥 FIX SPARK)
# =================================================
joined = (
    pred.alias("p")
    .join(
        kline.alias("k"),
        (F.col("k.k_time") > F.col("p.predicting_at")) &
        (
            F.col("k.k_time") <=
            F.expr("timestampadd(HOUR, p.lookahead_h, p.predicting_at)")
        ),
        "inner"
    )
)

# =================================================
# AGG FUTURE MOVE
# =================================================
agg = (
    joined
    .groupBy(
        "prediction_id",
        "action",
        "entry_price",
        "buy_is_runner",
        "sell_is_strong"
    )
    .agg(
        F.max(
            (F.col("high_price") - F.col("entry_price")) / F.col("entry_price")
        ).alias("max_ret"),
        F.min(
            (F.col("low_price") - F.col("entry_price")) / F.col("entry_price")
        ).alias("min_ret"),
        F.max("k_time").alias("confirmed_at")
    )
)

# =================================================
# RESULT LOGIC + EXIT PRICE
# =================================================
result = (
    agg
    .withColumn(
        "exit_pct",
        F.when(
            (F.col("action") == "BUY") & (F.col("buy_is_runner")) & (F.col("max_ret") >= BUY_TP_RUN),
            BUY_TP_RUN
        ).when(
            (F.col("action") == "BUY") & (F.col("max_ret") >= BUY_TP_BASE),
            BUY_TP_BASE
        ).when(
            (F.col("action") == "BUY") & (F.col("min_ret") <= -BUY_SL),
            -BUY_SL
        ).when(
            (F.col("action") == "SELL") & (F.col("sell_is_strong")) & (F.col("min_ret") <= -SELL_TP_STRONG),
            SELL_TP_STRONG
        ).when(
            (F.col("action") == "SELL") & (F.col("min_ret") <= -SELL_TP_WEAK),
            SELL_TP_WEAK
        ).when(
            (F.col("action") == "SELL") & (~F.col("sell_is_strong")) & (F.col("max_ret") >= SELL_SL_WEAK),
            -SELL_SL_WEAK
        ).otherwise(-SELL_SL_STRONG)
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

# =================================================
# FINAL OUTPUT
# =================================================
final_df = result.select(
    "prediction_id",
    "result_status",
    "pnl_pct",
    "exit_price",
    "confirmed_at"
)

# =================================================
# SAFE WRITE
# =================================================
final_new = (
    final_df
    .join(
        confirmed.select("prediction_id"),
        "prediction_id",
        "left_anti"
    )
)

if final_new.rdd.isEmpty():
    print("ℹ️ No new BTC confirms – safe skip")
else:
    (
        final_new
        .write
        .mode("append")
        .jdbc(url, "fact_prediction_result", properties=jdbc_props)
    )
    print("✅ BTC LOW FREQ confirm inserted safely")

spark.stop()
