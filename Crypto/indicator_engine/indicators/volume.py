from pyspark.sql import functions as F, Window
from core.registry import register


# =====================
# OBV (Cumulative)
# =====================
@register("OBV")
def obv(df, w):

    prev = F.lag("close_price").over(w)

    step = (
        F.when(F.col("close_price") > prev,  F.col("volume"))
         .when(F.col("close_price") < prev, -F.col("volume"))
         .otherwise(0)
    )

    wc = w.rowsBetween(Window.unboundedPreceding, 0)

    obv_val = F.sum(step).over(wc)

    return df.select(
        "symbol_id",
        "interval_id",

        F.lit("OBV").alias("type_name"),
        obv_val.alias("value"),

        F.col("close_time").alias("timestamp")
    )


# =====================
# Volume MA20
# =====================
@register("VOL_MA20")
def vol_ma20(df, w):

    w20 = w.rowsBetween(-19, 0)

    ma = F.avg("volume").over(w20)

    return df.select(
        "symbol_id",
        "interval_id",

        F.lit("VOL_MA20").alias("type_name"),
        ma.alias("value"),

        F.col("close_time").alias("timestamp")
    )


# =====================
# VWAP Daily
# =====================
@register("VWAP_D")
def vwap_d(df, w):

    df2 = df.withColumn("day", F.to_date("close_time"))

    wd = (
        Window
        .partitionBy("symbol_id", "interval_id", "day")
        .orderBy("close_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    pv = F.col("close_price") * F.col("volume")

    vwap = F.sum(pv).over(wd) / F.sum("volume").over(wd)

    return df2.select(
        "symbol_id",
        "interval_id",

        F.lit("VWAP_D").alias("type_name"),
        vwap.alias("value"),

        F.col("close_time").alias("timestamp")
    )


# =====================
# Volume Profile (Bucket 10)
# =====================
@register("VP_10")
def volume_profile_10(df, w):

    bucket = F.round(F.col("close_price"), -1)

    wb = Window.partitionBy(
        "symbol_id",
        "interval_id",
        bucket
    )

    vp = F.sum("volume").over(wb)

    return df.select(
        "symbol_id",
        "interval_id",

        F.lit("VP_10").alias("type_name"),
        vp.alias("value"),

        F.col("close_time").alias("timestamp")
    )

# =====================
# VWAP Deviation
# =====================
@register("VWAP_DEV")
def vwap_dev(df, w):

    # VWAP Daily window
    df2 = df.withColumn("day", F.to_date("close_time"))

    wd = (
        Window
        .partitionBy("symbol_id", "interval_id", "day")
        .orderBy("close_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    pv = F.col("close_price") * F.col("volume")

    vwap = (
        F.sum(pv).over(wd) /
        F.sum("volume").over(wd)
    )

    dev = F.when(
        (vwap.isNull()) | (vwap == 0),
        None
    ).otherwise(
        (F.col("close_price") - vwap) / vwap
    )

    return df2.select(
        "symbol_id",
        "interval_id",

        F.lit("VWAP_DEV").alias("type_name"),
        dev.alias("value"),

        "close_time"
    )