from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date
import sys

# =====================================================
# SPARK SESSION
# =====================================================
spark = (SparkSession.builder
    .appName("RawToStaging")
    .config("spark.sql.session.timeZone", "UTC")
    # LIMIT MEMORY
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")

    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# AIRFLOW PARAMS
# =====================================================
execution_date = spark.conf.get("spark.airflow.execution_date")

if not execution_date:
    print("❌ execution_date missing")
    sys.exit(1)

# =====================================================
# PATH
# =====================================================
RAW_PATH = "/root/crypto_ubuntu/data_lake/raw/crypto"
STAGING_PATH = "/root/crypto_ubuntu/data_lake/staging/crypto"

# =====================================================
# READ + TRANSFORM + FILTER (EARLY)
# =====================================================
df = spark.read.parquet(RAW_PATH)

df = (
    df
    .withColumn("open_time", from_unixtime(col("open_time") / 1000))
    .withColumn("close_time", from_unixtime(col("close_time") / 1000))
    .withColumn("dt", to_date(col("fetched_at")))
    .filter(to_date(col("fetched_at")) == execution_date)
)

rows = df.count()
print(f"Rows to staging = {rows}")

# =====================================================
# WRITE (IDEMPOTENT)
# =====================================================
if rows > 0:
    df.write \
        .mode("append") \
        .partitionBy("symbol", "interval", "dt") \
        .parquet(STAGING_PATH)
        
    print("✅ STAGING WRITE DONE")
else:
    print("ℹ️ No data")

spark.stop()
