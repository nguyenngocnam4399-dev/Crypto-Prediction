from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
import sys

spark = (SparkSession.builder
    .appName("StagingToFact")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")

    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================
# PATH
# =========================
STAGING_PATH = "/root/crypto_ubuntu/data_lake/staging/crypto"

# =========================
# READ STAGING
# =========================
staging_df = spark.read.parquet(STAGING_PATH)
print(f"STAGING rows = {staging_df.count()}")

# =========================
# MYSQL CONFIG
# =========================
jdbc_url = (
    "jdbc:mysql://localhost:3306/crypto_dw"
    "?useSSL=false&allowPublicKeyRetrieval=true"
    "&serverTimezone=UTC"
)

props = {
    "user": "spark_user",
    "password": "spark123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# =====================================================
# READ STAGING
# =====================================================
staging_df = spark.read.parquet(STAGING_PATH)

staging_df = staging_df.persist()

staging_cnt = staging_df.count()

print(f"📥 STAGING rows = {staging_cnt}", flush=True)

if staging_cnt == 0:
    print("ℹ️ No staging data", flush=True)
    spark.stop()
    sys.exit(0)

# =========================
# READ DIM
# =========================
dim_symbol = broadcast(
    spark.read.jdbc(jdbc_url, "dim_symbol", properties=props)
)

dim_interval = broadcast(
    spark.read.jdbc(jdbc_url, "dim_interval", properties=props)
)

# =========================
# JOIN STAGING → DIM
# =========================
fact_candidate = staging_df \
    .join(dim_symbol, staging_df.symbol == dim_symbol.symbol_name) \
    .join(dim_interval, staging_df.interval == dim_interval.interval_name) \
    .select(
        col("symbol_id"),
        col("interval_id"),
        col("open").cast("decimal(20,10)").alias("open_price"),
        col("high").cast("decimal(20,10)").alias("high_price"),
        col("low").cast("decimal(20,10)").alias("low_price"),
        col("close").cast("decimal(20,10)").alias("close_price"),
        col("volume").cast("decimal(38,18)").alias("volume"),
        col("open_time"),
        col("close_time")
    ) \
    .dropDuplicates(["symbol_id", "interval_id", "open_time"])

fact_candidate = fact_candidate.persist()

cand_cnt = fact_candidate.count()
print(f"FACT candidate rows = {fact_candidate.count()}")

# =========================
# READ EXISTING FACT KEYS
# =========================
fact_existing = spark.read.jdbc(
    jdbc_url,
    "(SELECT symbol_id, interval_id, open_time FROM fact_kline) f",
    properties=props
)

# =========================
# ANTI JOIN (LOẠI BỎ DUP)
# =========================
fact_new = fact_candidate.join(
    fact_existing,
    ["symbol_id", "interval_id", "open_time"],
    "left_anti"
)

new_cnt = fact_new.count()
print(f"FACT new rows (to insert) = {new_cnt}")

# =========================
# WRITE FACT (SAFE)
# =========================
if new_cnt > 0:
    fact_new.repartition(4).write \
        .mode("append") \
        .option("batchsize", "2000") \
        .option("isolationLevel", "READ_COMMITTED") \
        .jdbc(jdbc_url, "fact_kline", properties=props)
    print("=> FACT INSERTED")
else:
    print("ℹ️ No new rows to insert")

spark.stop()

