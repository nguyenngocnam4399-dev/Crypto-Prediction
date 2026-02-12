from pyspark.sql import functions as F
from core.registry import register
from indicators.trend_utils import ema


# ================= ADX 14 (WILDER - CLEAN) =================

@register("ADX14")
def adx14(df, w):

    prev_high = F.lag("high_price").over(w)
    prev_low = F.lag("low_price").over(w)
    prev_close = F.lag("close_price").over(w)

    # True Range
    tr = F.greatest(
        F.col("high_price") - F.col("low_price"),
        F.abs(F.col("high_price") - prev_close),
        F.abs(F.col("low_price") - prev_close)
    )

    # Directional Move
    up = F.col("high_price") - prev_high
    down = prev_low - F.col("low_price")

    plus_dm = F.when((up > down) & (up > 0), up).otherwise(0.0)
    minus_dm = F.when((down > up) & (down > 0), down).otherwise(0.0)

    # ========== STAGING COLUMNS ==========
    df2 = (
        df
        .withColumn("tr", tr)
        .withColumn("plus_dm", plus_dm)
        .withColumn("minus_dm", minus_dm)
    )

    # ========== Wilder smoothing ==========
    df2 = (
        df2
        .withColumn("atr", ema("tr", 14))
        .withColumn("sm_plus", ema("plus_dm", 14))
        .withColumn("sm_minus", ema("minus_dm", 14))
    )

    # DI
    df2 = df2.withColumn(
        "plus_di",
        F.when(F.col("atr") == 0, None)
         .otherwise(100.0 * F.col("sm_plus") / F.col("atr"))
    )

    df2 = df2.withColumn(
        "minus_di",
        F.when(F.col("atr") == 0, None)
         .otherwise(100.0 * F.col("sm_minus") / F.col("atr"))
    )

    # DX
    df2 = df2.withColumn(
        "dx",
        F.when(
            (F.col("plus_di") + F.col("minus_di")) == 0,
            0.0
        ).otherwise(
            100.0 * F.abs(F.col("plus_di") - F.col("minus_di")) /
            (F.col("plus_di") + F.col("minus_di"))
        )
    )

    # ADX
    df2 = df2.withColumn("adx", ema("dx", 14))

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("ADX14").alias("type_name"),
        F.col("adx").alias("value"),
        "close_time"
    )
