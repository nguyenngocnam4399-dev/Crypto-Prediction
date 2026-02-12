# core/writer.py

from pyspark.sql import functions as F
import config


def save_indicators(df, spark):

    jdbc_url = (
        f"jdbc:mysql://{config.DB_CONFIG['host']}:{config.DB_CONFIG['port']}/"
        f"{config.DB_CONFIG['database']}?useSSL=false"
    )

    props = {
        "user": config.DB_CONFIG["user"],
        "password": config.DB_CONFIG["password"],
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    dim = spark.read.jdbc(
        jdbc_url,"dim_indicator_type",properties=props
    )

    df = (
        df.join(dim,"type_name")
          .select("symbol_id","interval_id","type_id","value","timestamp")
    )

    exist = spark.read.jdbc(
        jdbc_url,
        "(SELECT symbol_id,interval_id,type_id,timestamp FROM fact_indicator) t",
        properties=props
    )

    new = (df.join(
        exist,
        ["symbol_id","interval_id","type_id","timestamp"],
        "left_anti"
    ).cache())
    cnt = new.count()
    if cnt>0:

        new.write.mode("append").jdbc(
            jdbc_url,"fact_indicator",properties=props
        )

        print("✅ INSERTED:",{cnt})

    else:
        print("ℹ️ NO NEW DATA")
