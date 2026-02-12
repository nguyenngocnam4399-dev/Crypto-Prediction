from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType

# =========================================================
# SPARK SESSION
# =========================================================
spark = (
    SparkSession.builder
    .appName("NewsCoinMapping")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# =========================================================
# JDBC CONFIG
# =========================================================
jdbc_url = (
    "jdbc:mysql://localhost:3306/crypto_dw"
    "?useSSL=false&allowPublicKeyRetrieval=true"
)

props = {
    "user": "spark_user",
    "password": "spark123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# =========================================================
# LOAD TABLES
# =========================================================
news_df = spark.read.jdbc(jdbc_url, "news_fact", properties=props)
symbol_df = spark.read.jdbc(jdbc_url, "dim_symbol", properties=props)

# =========================================================
# COIN KEYWORDS (RULE-BASED – CỰC KỲ QUAN TRỌNG)
# =========================================================
COIN_KEYWORDS = {
    "BTC": ["BTC", "BITCOIN"],
    "ETH": ["ETH", "ETHEREUM"],
    "BNB": ["BNB", "BINANCE"]
}

def extract_coins(title: str):
    if title is None:
        return []
    title = title.upper()
    found = []
    for coin, keywords in COIN_KEYWORDS.items():
        for k in keywords:
            if k in title:
                found.append(coin)
                break
    return found

extract_udf = F.udf(extract_coins, ArrayType(StringType()))

# =========================================================
# EXTRACT COINS FROM NEWS TITLE
# =========================================================
news_coin_raw = (
    news_df
    .withColumn("coins", extract_udf(F.col("title")))
    .withColumn("coin", F.explode("coins"))
)

# =========================================================
# MAP TO symbol_dim
# symbol_dim.symbol_name = BTCUSDT → lấy BTC
# =========================================================
symbol_map = (
    symbol_df
    .withColumn("coin",
        F.regexp_replace(F.col("symbol_name"), "USDT", "")
    )
)

news_coin_final = (
    news_coin_raw
    .join(symbol_map, "coin", "inner")
    .select(
        F.col("id").alias("news_id"),
        F.col("symbol_id"),
        F.lit(1.0).alias("confidence")
    )
    .dropDuplicates(["news_id", "symbol_id"])
)

# =========================================================
# REMOVE DUPLICATES (ANTI JOIN)
# =========================================================
existing_df = spark.read.jdbc(
    jdbc_url, "news_coin_fact", properties=props
)

news_coin_new = news_coin_final.join(
    existing_df,
    ["news_id", "symbol_id"],
    "left_anti"
)

print(f"🆕 New news-coin rows = {news_coin_new.count()}")

# =========================================================
# WRITE TO news_coin_fact
# =========================================================
if news_coin_new.count() > 0:
    (
        news_coin_new
        .write
        .mode("append")
        .jdbc(jdbc_url, "news_coin_fact", properties=props)
    )
    print("✅ news_coin_fact inserted")
else:
    print("ℹ️ No new rows to insert")

spark.stop()
