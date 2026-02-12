# core/loader.py

from pyspark.sql import SparkSession, functions as F
import config


def load_kline(spark, symbol=None, interval=None):

    jdbc_url = (
        f"jdbc:mysql://{config.DB_CONFIG['host']}:{config.DB_CONFIG['port']}/"
        f"{config.DB_CONFIG['database']}?useSSL=false"
    )

    props = {
        "user": config.DB_CONFIG["user"],
        "password": config.DB_CONFIG["password"],
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    query = """
    (
        SELECT
            symbol_id,
            interval_id,
            close_time,
            close_price,
            high_price,
            low_price,
            volume
        FROM fact_kline
        WHERE close_time <= UTC_TIMESTAMP()
    ) k
    """

    df = spark.read.jdbc(jdbc_url, query, properties=props)

    if symbol:
        df = df.filter(F.col("symbol_id")==symbol)

    if interval:
        df = df.filter(F.col("interval_id")==interval)

    return df.cache()
