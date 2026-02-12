from pyspark.sql import functions as F


def ema(col, period):

    alpha = 2/(period+1)

    return F.expr(f"""
    aggregate(
      collect_list({col}) OVER (
        PARTITION BY symbol_id,interval_id
        ORDER BY close_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
      CAST(NULL AS DOUBLE),
      (a,x)->CASE
        WHEN a IS NULL THEN x
        ELSE a+{alpha}*(x-a)
      END
    )
    """)
