from pyspark.sql import functions as F
from core.registry import register
from indicators.trend_utils import ema


# ======================================================
# SUPER TREND (10,3) - STATEFUL (FIXED)
# ======================================================
@register("SUPER_TREND_10_3")
def supertrend_10_3(df, w):

    prev_close = F.lag("close_price").over(w)

    # True Range
    tr = F.greatest(
        F.col("high_price") - F.col("low_price"),
        F.abs(F.col("high_price") - prev_close),
        F.abs(F.col("low_price") - prev_close)
    )

    atr = ema(tr, 14)

    hl2 = (F.col("high_price") + F.col("low_price")) / 2

    ub = hl2 + 3 * atr
    lb = hl2 - 3 * atr

    st = F.expr(f"""
    aggregate(
      collect_list(
        struct(
          close_price as close,
          {ub._jc.toString()} as ub,
          {lb._jc.toString()} as lb
        )
      ) OVER (
        PARTITION BY symbol_id, interval_id
        ORDER BY close_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),

      struct(
        cast(null as double) as fub,
        cast(null as double) as flb,
        cast(1 as int) as trend
      ),

      (s,x) -> struct(

        /* Final Upper Band */
        CASE
          WHEN s.fub IS NULL THEN x.ub
          WHEN x.ub < s.fub OR x.close > s.fub
          THEN x.ub
          ELSE s.fub
        END,

        /* Final Lower Band */
        CASE
          WHEN s.flb IS NULL THEN x.lb
          WHEN x.lb > s.flb OR x.close < s.flb
          THEN x.lb
          ELSE s.flb
        END,

        /* Trend */
        CASE
          WHEN x.close > s.fub THEN 1
          WHEN x.close < s.flb THEN -1
          ELSE s.trend
        END
      )
    ).trend
    """)

    return df.select(
        "symbol_id",
        "interval_id",
        F.lit("SUPER_TREND_10_3").alias("type_name"),
        st.alias("value"),
        "close_time"
    )
