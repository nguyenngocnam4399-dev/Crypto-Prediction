# #FP-Growth cho bạn pattern phổ biến, không phải pattern hiệu quả
# from pyspark.sql import SparkSession, functions as F

# # =========================================================
# # SPARK SESSION
# # =========================================================
# spark = (
#     SparkSession.builder
#     .appName("Metric_Frequency_WIN_Radar6_Norm")
#     .config("spark.sql.shuffle.partitions", "8")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")
# spark.conf.set("spark.sql.session.timeZone", "UTC")

# # =========================================================
# # JDBC CONFIG
# # =========================================================
# jdbc_url = (
#     "jdbc:mysql://localhost:3306/crypto_dw"
#     "?useSSL=false&allowPublicKeyRetrieval=true"
# )

# props = {
#     "user": "spark_user",
#     "password": "spark123",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # =========================================================
# # READ TABLES
# # =========================================================
# prediction_df = spark.read.jdbc(
#     jdbc_url, "fact_prediction", properties=props
# )

# result_df = spark.read.jdbc(
#     jdbc_url, "fact_prediction_result", properties=props
# )

# # =========================================================
# # FILTER WIN TRADES
# # =========================================================
# win_df = (
#     prediction_df.alias("p")
#     .join(
#         result_df.alias("r"),
#         F.col("p.id") == F.col("r.prediction_id"),
#         "inner"
#     )
#     .filter(F.col("r.result_status") == "WIN")
# )

# # =========================================================
# # TOTAL WIN PER SYMBOL
# # =========================================================
# total_win = (
#     win_df
#     .groupBy("p.symbol_id")
#     .agg(F.count("*").alias("total_win_trades"))
# )

# # =========================================================
# # BUILD RADAR METRICS (DERIVED, SCHEMA-SAFE)
# # 6 TRỤC RADAR
# # =========================================================
# metrics = (
#     win_df
#     .select(
#         F.col("p.symbol_id"),

#         # 1️⃣ Market score
#         F.when(F.col("p.market_score") > 0, 1).otherwise(0).alias("MARKET_SCORE_POS"),
#         F.when(F.col("p.market_score") < 0, 1).otherwise(0).alias("MARKET_SCORE_NEG"),

#         # 2️⃣ Market bias
#         F.when(F.col("p.market_bias") == "BULL", 1).otherwise(0).alias("BIAS_BULL"),

#         # 3️⃣ Signal direction
#         F.when(F.col("p.signal_type") == "BUY", 1).otherwise(0).alias("BUY_RATIO"),

#         # 4️⃣ Risk / Reward
#         F.when(
#             (F.col("p.take_profit_price") - F.col("p.entry_price"))
#             / (F.col("p.entry_price") - F.col("p.stop_loss_price")) > 1,
#             1
#         ).otherwise(0).alias("RR_GT_1"),

#         F.when(
#             (F.col("p.take_profit_price") - F.col("p.entry_price"))
#             / (F.col("p.entry_price") - F.col("p.stop_loss_price")) > 2,
#             1
#         ).otherwise(0).alias("RR_GT_2")
#     )
# )

# # =========================================================
# # UNPIVOT METRICS
# # =========================================================
# metric_cols = [c for c in metrics.columns if c != "symbol_id"]

# long_df = (
#     metrics
#     .select(
#         "symbol_id",
#         F.expr(
#             "stack("
#             + str(len(metric_cols))
#             + ", "
#             + ", ".join([f"'{c}', {c}" for c in metric_cols])
#             + ") as (metric_name, metric_flag)"
#         )
#     )
#     .filter(F.col("metric_flag") == 1)
# )

# # =========================================================
# # COUNT FREQUENCY
# # =========================================================
# freq_df = (
#     long_df
#     .groupBy("symbol_id", "metric_name")
#     .agg(F.count("*").alias("win_trade_count"))
# )

# # =========================================================
# # CALCULATE PERCENTAGE + NORMALIZE [0–1]
# # =========================================================
# final_df = (
#     freq_df
#     .join(total_win, "symbol_id")
#     .withColumn(
#         "frequency_pct",
#         F.round(
#             F.col("win_trade_count") / F.col("total_win_trades") * 100, 2
#         )
#     )
#     .withColumn(
#         "frequency_norm",
#         F.round(F.col("frequency_pct") / 100, 4)
#     )
# )

# # =========================================================
# # WRITE RESULT
# # =========================================================
# final_df.write.mode("overwrite").jdbc(
#     jdbc_url,
#     "metric_frequency_win",
#     properties=props
# )

# print("✅ metric_frequency_win generated (radar 6 metrics, normalized 0–1)")

# spark.stop()


# =========================================================
# spark_job_fp_growth.py
# FP-GROWTH WIN PATTERN MINING (CONFIGURABLE, PROD)
# =========================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.fpm import FPGrowth
import sys

# =========================================================
# ======================= CONFIG ==========================
# =========================================================

# ---- JDBC ----
DB = {
    "host": "localhost",
    "port": "3306",
    "db": "crypto_dw",
    "user": "spark_user",
    "password": "spark123"
}

JDBC_URL = (
    f"jdbc:mysql://{DB['host']}:{DB['port']}/{DB['db']}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

JDBC_PROPS = {
    "user": DB["user"],
    "password": DB["password"],
    "driver": "com.mysql.cj.jdbc.Driver",
    "rewriteBatchedStatements": "true"
}

# ---- FP-GROWTH PARAM ----
MIN_SUPPORT    = 0.02      # >= 2% win trades
MIN_CONFIDENCE = 0.6
MIN_WIN_TRADES = 50        # skip coin quá ít data

# ---- FILTER ----
USE_METRIC_NO_TRADE_ONLY = True   # chỉ dùng trade được phép
ACTION_WHITELIST = ["BUY", "SELL"]

# ---- OUTPUT TABLE ----
TABLE_PATTERN = "fp_growth_win_patterns"
TABLE_RULE    = "fp_growth_win_rules"

# =========================================================
# ===================== SPARK ==============================
# =========================================================
spark = (
    SparkSession.builder
    .appName("FP_GROWTH_WIN_CONFIGURABLE")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# =========================================================
# ===================== LOAD DATA ==========================
# =========================================================
prediction = spark.read.jdbc(JDBC_URL, "fact_prediction", properties=JDBC_PROPS)
result     = spark.read.jdbc(JDBC_URL, "fact_prediction_result", properties=JDBC_PROPS)

# =========================================================
# ===================== WIN TRADES =========================
# =========================================================
win_df = (
    prediction.alias("p")
    .join(
        result.alias("r"),
        F.col("p.id") == F.col("r.prediction_id"),
        "inner"
    )
    .filter(F.col("r.result_status") == "WIN")
)

if USE_METRIC_NO_TRADE_ONLY:
    win_df = win_df.filter(F.col("p.metric_no_trade") == 0)

win_df = win_df.filter(F.col("p.action").isin(ACTION_WHITELIST))

# =========================================================
# ================= BUILD TRANSACTIONS ====================
# =========================================================
txn = (
    win_df
    .select(
        F.col("p.symbol_id"),

        F.array_distinct(
            F.array(
                # TREND
                F.when(F.col("p.metric_trend") > 0,  F.lit("TREND_POS")),
                F.when(F.col("p.metric_trend") < 0,  F.lit("TREND_NEG")),

                # MOMENTUM
                F.when(F.col("p.metric_momentum") > 0, F.lit("MOMENTUM_POS")),
                F.when(F.col("p.metric_momentum") < 0, F.lit("MOMENTUM_NEG")),

                # VOLATILITY
                F.when(F.col("p.metric_volatility") > 0, F.lit("VOLATILITY_POS")),
                F.when(F.col("p.metric_volatility") < 0, F.lit("VOLATILITY_NEG")),

                # ACTION
                F.when(F.col("p.action") == "BUY",  F.lit("ACTION_BUY")),
                F.when(F.col("p.action") == "SELL", F.lit("ACTION_SELL"))
            )
        ).alias("items")
    )
    .withColumn("items", F.expr("filter(items, x -> x is not null)"))
    .withColumn("items", F.sort_array("items"))
)

# =========================================================
# ================= TOTAL WIN PER SYMBOL ==================
# =========================================================
total_win = (
    txn
    .groupBy("symbol_id")
    .agg(F.count("*").alias("total_win_trades"))
)

# =========================================================
# ================= FP-GROWTH PER SYMBOL ==================
# =========================================================
symbols = [r.symbol_id for r in txn.select("symbol_id").distinct().collect()]

pattern_list = []
rule_list = []

for sid in symbols:
    tx = txn.filter(F.col("symbol_id") == sid)
    win_count = tx.count()

    if win_count < MIN_WIN_TRADES:
        continue

    fp = FPGrowth(
        itemsCol="items",
        minSupport=MIN_SUPPORT,
        minConfidence=MIN_CONFIDENCE
    )

    model = fp.fit(tx)

    # -------- PATTERNS --------
    patterns = (
        model.freqItemsets
        .withColumn("symbol_id", F.lit(sid))
    )

    # -------- RULES --------
    rules = (
        model.associationRules
        .withColumn("symbol_id", F.lit(sid))
    )

    pattern_list.append(patterns)
    rule_list.append(rules)

if not pattern_list:
    print("❌ No symbol đủ điều kiện FP-Growth")
    spark.stop()
    sys.exit(0)

# =========================================================
# ================= UNION ALL ===============================
# =========================================================
patterns_df = pattern_list[0]
for p in pattern_list[1:]:
    patterns_df = patterns_df.unionByName(p)

rules_df = rule_list[0]
for r in rule_list[1:]:
    rules_df = rules_df.unionByName(r)

# =========================================================
# ================= FINAL PATTERNS =========================
# =========================================================
patterns_out = (
    patterns_df
    .join(total_win, "symbol_id")
    .withColumn(
        "support_pct",
        F.round(F.col("freq") / F.col("total_win_trades") * 100, 2)
    )
    .withColumn("items", F.to_json("items"))   # JDBC SAFE
    .select(
        "symbol_id",
        "items",
        "freq",
        "total_win_trades",
        "support_pct"
    )
)

# =========================================================
# ================= FINAL RULES ============================
# =========================================================
rules_out = (
    rules_df
    .withColumn("antecedent", F.to_json("antecedent"))
    .withColumn("consequent", F.to_json("consequent"))
    .select(
        "symbol_id",
        "antecedent",
        "consequent",
        F.round("confidence", 4).alias("confidence"),
        F.round("lift", 4).alias("lift")
    )
)

# =========================================================
# ================= WRITE MYSQL ============================
# =========================================================
patterns_out.write \
    .mode("overwrite") \
    .jdbc(JDBC_URL, TABLE_PATTERN, properties=JDBC_PROPS)

rules_out.write \
    .mode("overwrite") \
    .jdbc(JDBC_URL, TABLE_RULE, properties=JDBC_PROPS)

print("✅ FP-GROWTH WIN PATTERN + RULE DONE (CONFIGURABLE)")

spark.stop()
