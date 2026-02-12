from pyspark.sql import functions as F
from core.registry import register
from indicators.trend_utils import ema


# ================= ATR % =================

@register("ATR_PCT")
def atr_pct(df, w):

    prev_close = F.lag("close_price").over(w)

    tr = F.greatest(
        F.col("high_price") - F.col("low_price"),
        F.abs(F.col("high_price") - prev_close),
        F.abs(F.col("low_price") - prev_close)
    )

    atr = ema(tr, 14)

    val = F.when(
        F.col("close_price") == 0,
        None
    ).otherwise(
        atr / F.col("close_price") * 100
    )

    return df.select(
        "symbol_id",
        "interval_id",
        F.lit("ATR_PCT").alias("type_name"),
        val.alias("value"),
        "close_time"
    )


# ================= BB WIDTH =================

@register("BB_WIDTH")
def bb_width(df, w):

    wn = w.rowsBetween(-19, 0)

    ma = F.avg("close_price").over(wn)
    std = F.stddev("close_price").over(wn)

    upper = ma + 2 * std
    lower = ma - 2 * std

    val = F.when(
        ma == 0,
        None
    ).otherwise(
        (upper - lower) / ma
    )

    return df.select(
        "symbol_id",
        "interval_id",
        F.lit("BB_WIDTH").alias("type_name"),
        val.alias("value"),
        "close_time"
    )
# ================= BB UPPER =================

@register("BB_UP20")
def bb_up20(df, w):

    wn = w.rowsBetween(-19, 0)

    ma = F.avg("close_price").over(wn)
    std = F.stddev("close_price").over(wn)

    upper = ma + 2 * std

    return df.select(
        "symbol_id","interval_id",
        F.lit("BB_UP20").alias("type_name"),
        upper.alias("value"),
        "close_time"
    )


# ================= BB LOWER =================

@register("BB_DN20")
def bb_dn20(df, w):

    wn = w.rowsBetween(-19, 0)

    ma = F.avg("close_price").over(wn)
    std = F.stddev("close_price").over(wn)

    lower = ma - 2 * std

    return df.select(
        "symbol_id","interval_id",
        F.lit("BB_DN20").alias("type_name"),
        lower.alias("value"),
        "close_time"
    )

# ================= ATR 14 =================

@register("ATR14")
def atr14(df, w):

    prev_close = F.lag("close_price").over(w)

    # True Range
    tr = F.greatest(
        F.col("high_price") - F.col("low_price"),
        F.abs(F.col("high_price") - prev_close),
        F.abs(F.col("low_price") - prev_close)
    )

    # Stage column (IMPORTANT)
    df2 = df.withColumn("tr", tr)

    # Wilder ATR
    df2 = df2.withColumn("atr14", ema("tr", 14))

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("ATR14").alias("type_name"),
        F.col("atr14").alias("value"),
        "close_time"
    )
