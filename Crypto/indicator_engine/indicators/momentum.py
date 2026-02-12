from pyspark.sql import functions as F
from pyspark.sql.window import Window
from core.registry import register


# ======================================================
# SAFE DIV
# ======================================================
def safe_div(a, b):
    return F.when(b == 0, None).otherwise(a / b)


# ======================================================
# EMA CORE (STRING COLUMN ONLY)
# ======================================================
def ema(col, period):

    alpha = 2.0 / (period + 1.0)

    return F.expr(f"""
    aggregate(
      collect_list({col}) OVER (
        PARTITION BY symbol_id, interval_id
        ORDER BY close_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
      CAST(NULL AS DOUBLE),
      (a,x)->CASE
        WHEN a IS NULL THEN x
        ELSE a + {alpha} * (x - a)
      END
    )
    """)


# ======================================================
# RSI WILDER (REFACTORED)
# ======================================================
def rsi_wilder(df, w, period):

    diff = F.col("close_price") - F.lag("close_price").over(w)

    df2 = (
        df
        .withColumn("gain", F.when(diff > 0, diff).otherwise(0.0))
        .withColumn("loss", F.when(diff < 0, -diff).otherwise(0.0))
    )

    df2 = (
        df2
        .withColumn("avg_gain", ema("gain", period))
        .withColumn("avg_loss", ema("loss", period))
    )

    rs = safe_div(F.col("avg_gain"), F.col("avg_loss"))

    df2 = df2.withColumn(
        "rsi",
        100.0 - (100.0 / (1.0 + rs))
    )

    return df2



# ======================================================
# RSI 14
# ======================================================
@register("RSI14")
def rsi14(df, w):

    df2 = rsi_wilder(df, w, 14)

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("RSI14").alias("type_name"),
        F.col("rsi").alias("value"),
        "close_time"
    )



# ======================================================
# RSI 21
# ======================================================
@register("RSI21")
def rsi21(df, w):

    df2 = rsi_wilder(df, w, 21)

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("RSI21").alias("type_name"),
        F.col("rsi").alias("value"),
        "close_time"
    )



# ======================================================
# MACD PIPELINE
# ======================================================
def macd_pipeline(df):

    return (
        df
        .withColumn("ema12", ema("close_price", 12))
        .withColumn("ema26", ema("close_price", 26))
        .withColumn("macd", F.col("ema12") - F.col("ema26"))
        .withColumn("signal", ema("macd", 9))
        .withColumn("hist", F.col("macd") - F.col("signal"))
    )


# ======================================================
# MACD
# ======================================================
@register("MACD")
def macd(df, w):

    df2 = macd_pipeline(df)

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("MACD").alias("type_name"),
        F.col("macd").alias("value"),
        "close_time"
    )


# ======================================================
# MACD SIGNAL
# ======================================================
@register("MACD_SIGNAL")
def macd_signal(df, w):

    df2 = macd_pipeline(df)

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("MACD_SIGNAL").alias("type_name"),
        F.col("signal").alias("value"),
        "close_time"
    )


# ======================================================
# MACD HIST
# ======================================================
@register("MACD_HIST")
def macd_hist(df, w):

    df2 = macd_pipeline(df)

    return df2.select(
        "symbol_id",
        "interval_id",
        F.lit("MACD_HIST").alias("type_name"),
        F.col("hist").alias("value"),
        "close_time"
    )


# ======================================================
# STOCH RSI
# ======================================================
@register("STOCH_RSI")
def stoch_rsi(df, w):

    period = 14
    wn = w.rowsBetween(-(period - 1), 0)

    rsi_val = rsi_wilder(df, w, period)

    min_rsi = F.min(rsi_val).over(wn)
    max_rsi = F.max(rsi_val).over(wn)

    val = safe_div(
        (rsi_val - min_rsi),
        (max_rsi - min_rsi)
    )

    return df.select(
        "symbol_id",
        "interval_id",
        F.lit("STOCH_RSI").alias("type_name"),
        val.alias("value"),
        "close_time"
    )


# ======================================================
# CCI
# ======================================================
@register("CCI")
def cci(df, w):

    tp = (
        F.col("high_price") +
        F.col("low_price") +
        F.col("close_price")
    ) / 3.0

    period = 20
    wn = w.rowsBetween(-(period - 1), 0)

    ma = F.avg(tp).over(wn)
    md = F.avg(F.abs(tp - ma)).over(wn)

    val = safe_div(
        (tp - ma),
        (0.015 * md)
    )

    return df.select(
        "symbol_id",
        "interval_id",
        F.lit("CCI").alias("type_name"),
        val.alias("value"),
        "close_time"
    )
