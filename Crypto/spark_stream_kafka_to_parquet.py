from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = (SparkSession.builder
    .appName("KafkaToParquet-DE")
    .config("spark.sql.session.timeZone", "UTC")
    # ===== MEMORY CONTROL =====
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")

    # ===== PERFORMANCE =====
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")

    # ===== STABILITY =====
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("symbol", StringType()) \
    .add("interval", StringType()) \
    .add("open_time", LongType()) \
    .add("open", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low", DoubleType()) \
    .add("close", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("close_time", LongType()) \
    .add("fetched_at", StringType())

kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crypto-prediction")
    .option("startingOffsets", "earliest")
    # Avoid overload
    .option("maxOffsetsPerTrigger", "5000")
    .load()
)

parsed = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_filtered = parsed.filter(col("interval") == "2h")

query = parsed.writeStream \
    .format("parquet") \
    .option("path", "/root/crypto_ubuntu/data_lake/raw/crypto") \
    .option("checkpointLocation", "/root/crypto_ubuntu/data_lake/checkpoint/crypto") \
    .partitionBy("symbol", "interval") \
    .outputMode("append") \
    .start()

query.awaitTermination()

