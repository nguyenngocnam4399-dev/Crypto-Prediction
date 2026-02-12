# indicators/trend_filter.py

from pyspark.sql import functions as F
from core.registry import register
from indicators.trend_utils import ema


# ================= EMA =================

@register("EMA20")
def ema20(df,w):
    return _ma(df,"EMA20", ema("close_price",20))


@register("EMA50")
def ema50(df,w):
    return _ma(df,"EMA50", ema("close_price",50))


@register("EMA200")
def ema200(df,w):
    return _ma(df,"EMA200", ema("close_price",200))


# ================= SMA =================

@register("SMA50")
def sma50(df,w):

    w50 = w.rowsBetween(-49,0)

    return _ma(
        df,"SMA50",
        F.avg("close_price").over(w50)
    )


@register("SMA200")
def sma200(df,w):

    w200 = w.rowsBetween(-199,0)

    return _ma(
        df,"SMA200",
        F.avg("close_price").over(w200)
    )


# ================= HELPER =================

def _ma(df,name,col):

    return df.select(
        "symbol_id","interval_id",
        F.lit(name).alias("type_name"),
        col.alias("value"),
        "close_time"
    )
