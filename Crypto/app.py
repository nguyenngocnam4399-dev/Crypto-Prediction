from flask import Flask, jsonify, render_template, request
import pymysql
import time
# pip install numpy scikit-learn scipy
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from scipy import stats
# pip install mlxtend pandas
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules

app = Flask(__name__)

# Cấu hình MySQL
DB_CONFIG = {
    "host": "localhost",
    "user": "spark_user",
    "password": "spark123",
    "database": "crypto_dw",
    "cursorclass": pymysql.cursors.DictCursor
}


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/api/news/<symbol>")
def api_news_by_symbol(symbol):

    try:
        conn = pymysql.connect(**DB_CONFIG)

        # Chuẩn hoá giống prediction
        clean = symbol.upper()

        if not clean.endswith("USDT"):
            clean = f"{clean}USDT"

        sql = """
            SELECT
                n.title,
                n.sentiment_score,
                n.created_date,
                s.symbol_name
            FROM news_fact n

            JOIN news_coin_fact nc
                ON n.id = nc.news_id

            JOIN dim_symbol s
                ON nc.symbol_id = s.symbol_id

            WHERE s.symbol_name = %s

            ORDER BY n.created_date DESC
            LIMIT 5
        """

        with conn.cursor() as cur:
            cur.execute(sql, (clean,))
            rows = cur.fetchall()

        conn.close()

        result = []

        for r in rows:

            score = r["sentiment_score"]

            if score > 0:
                sentiment = "Positive"
            elif score < 0:
                sentiment = "Negative"
            else:
                sentiment = "Neutral"

            result.append({
                "title": r["title"],
                "sentiment": sentiment,
                "tag": r["symbol_name"],
                "time": r["created_date"].strftime("%H:%M")
            })

        return jsonify(result)

    except Exception as e:
        print("NEWS API ERROR:", e)
        return jsonify([])





@app.route("/")
def home():
    return render_template("prediction.html")

@app.route("/prediction")
def prediction():
    return render_template("prediction.html")

@app.route('/api/predictions/<symbol>')
def api_predictions(symbol):
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)

        clean_symbol = symbol.split(':')[-1].upper()
        target_symbol = clean_symbol if clean_symbol.endswith('USDT') else f"{clean_symbol}USDT"

        with conn.cursor() as cur:
            sql = """
            SELECT
                CONCAT(
                    DATE_FORMAT(p.predicting_at, '%%H:%%i'),
                    ' - ',
                    DATE_FORMAT(DATE_ADD(p.predicting_at, INTERVAL 2 HOUR), '%%H:%%i')
                ) AS `range`,

                p.action AS pred,

                -- Giá: ưu tiên giá thoát nếu có, không thì lấy entry
                COALESCE(r.exit_price, p.close, '--') AS price,

                -- Lợi nhuận
                CASE
                    WHEN r.pnl_pct IS NULL THEN '--'
                    ELSE CONCAT(
                        IF(r.pnl_pct > 0, '+', ''),
                        ROUND(r.pnl_pct * 100, 2),
                        '%%'
                    )
                END AS profit,

                -- Trạng thái
                CASE
                    WHEN p.action = 'SIDEWAY' THEN 'NA'
                    WHEN r.prediction_id IS NULL THEN 'PENDING'
                    ELSE r.result_status
                END AS status

            FROM fact_prediction p

            JOIN dim_symbol s
                ON p.symbol_id = s.symbol_id

            LEFT JOIN fact_prediction_result r
                ON p.id = r.prediction_id

            WHERE s.symbol_name = %s

            ORDER BY p.predicting_at DESC
            LIMIT 4;

            """
            cur.execute(sql, (target_symbol,))
            rows = cur.fetchall()
            return jsonify(rows)

    except Exception as e:
        print(f"Flask Error: {e}")
        return jsonify([])
    finally:
        if conn:
            conn.close()


@app.route("/history")
def history():
    return render_template("history.html")

@app.route("/api/history")
def api_history():
    conn = None

    try:
        conn = pymysql.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            sql = """
                SELECT
                    s.symbol_name AS symbol,

                    p.predicting_at AS prediction_at,

                    p.action AS pred,

                    p.close AS entry_price,

                    r.exit_price AS exit_price,

                    -- pnl hiển thị %
                    ROUND(r.pnl_pct * 100, 4) AS pnl_pct,

                    CASE
                        WHEN r.prediction_id IS NULL
                            AND NOW() < DATE_ADD(p.predicting_at, INTERVAL 2 HOUR)
                        THEN 'PENDING'

                        WHEN r.prediction_id IS NULL
                        THEN 'SIDEWAY'

                        ELSE r.result_status
                    END AS status

                FROM fact_prediction p

                JOIN dim_symbol s
                    ON p.symbol_id = s.symbol_id

                LEFT JOIN fact_prediction_result r
                    ON p.id = r.prediction_id

                WHERE p.action <> 'SIDEWAY'
                AND NOT (
                    r.prediction_id IS NULL
                    AND p.action <> 'SIDEWAY'
                )

                ORDER BY p.predicting_at DESC

                LIMIT 100;


            """
            cur.execute(sql)
            rows = cur.fetchall()

            result = []
            for r in rows:
                result.append({
                    "symbol": r["symbol"].replace("USDT", ""),
                    "time": r["prediction_at"].strftime("%Y-%m-%d %H:%M"),

                    # đổi pred -> signal
                    "signal": r["pred"],

                    # entry / exit
                    "entry": float(r["entry_price"]) if r["entry_price"] else None,
                    "exit": float(r["exit_price"]) if r["exit_price"] else None,

                    "pnl": None if r["pnl_pct"] is None else round(float(r["pnl_pct"]), 2),

                    # đổi status -> result
                    "result": r["status"]
                })


            return jsonify(result)

    except Exception as e:
        print("API /api/history error:", e)
        return jsonify([])

    finally:
        if conn:
            conn.close()


@app.route('/api/history/summary')
def api_history_summary():
    conn = pymysql.connect(**DB_CONFIG)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                -- Tổng WIN
                SUM(CASE WHEN e.result_status = 'WIN' THEN 1 ELSE 0 END) AS win,

                -- Tổng LOSS
                SUM(CASE WHEN e.result_status = 'LOSS' THEN 1 ELSE 0 END) AS loss,

                -- Tổng SIDEWAY (không trade)
                SUM(CASE WHEN p.action = 'SIDEWAY' THEN 1 ELSE 0 END) AS na,

                -- Accuracy = WIN / (WIN + LOSS)
                ROUND(
                    SUM(CASE WHEN e.result_status = 'WIN' THEN 1 ELSE 0 END)
                    /
                    NULLIF(
                        SUM(CASE WHEN e.result_status IN ('WIN','LOSS') THEN 1 ELSE 0 END),
                        0
                    )
                    * 100,
                    2
                ) AS accuracy

            FROM fact_prediction p

            LEFT JOIN fact_prediction_result e
                ON p.id = e.prediction_id
        """)

        row = cur.fetchone()

    conn.close()
    return jsonify(row)


@app.route('/api/history/accuracy-by-symbol')
def api_accuracy_by_symbol():
    conn = pymysql.connect(**DB_CONFIG)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                s.symbol_name,

                -- Tổng trade đã evaluate
                COUNT(*) AS total_trades,

                -- WIN / LOSS
                SUM(CASE WHEN e.result_status = 'WIN' THEN 1 ELSE 0 END) AS win,
                SUM(CASE WHEN e.result_status = 'LOSS' THEN 1 ELSE 0 END) AS loss,

                -- Accuracy = WIN / (WIN + LOSS)
                ROUND(
                    SUM(CASE WHEN e.result_status = 'WIN' THEN 1 ELSE 0 END)
                    /
                    NULLIF(COUNT(*), 0) * 100,
                    2
                ) AS accuracy,

                -- Trung bình PNL
                ROUND(AVG(e.pnl_pct) * 100, 4) AS avg_pnl

            FROM fact_prediction_result e

            JOIN fact_prediction p
                ON e.prediction_id = p.id

            JOIN dim_symbol s
                ON p.symbol_id = s.symbol_id

            WHERE
                e.result_status IN ('WIN','LOSS')

            GROUP BY
                s.symbol_name

            ORDER BY accuracy DESC
        """)

        rows = cur.fetchall()

    conn.close()
    return jsonify(rows)


@app.route("/analytics")
def analytics():
    return render_template("analytics.html")

@app.route("/api/analytics/regression")
def api_regression():

    symbol = request.args.get("symbol", "BTCUSDT")
    interval = request.args.get("interval", "2H")

    conn = None

    try:
        conn = pymysql.connect(**DB_CONFIG)

        with conn.cursor() as cur:

            # Map interval
            interval = request.args.get("interval", "2H").strip().lower()

            cur.execute("""
                SELECT interval_id
                FROM dim_interval
                WHERE LOWER(interval_name) = %s
                LIMIT 1
            """, (interval,))

            row = cur.fetchone()

            if not row:
                logger.warning(f"[REGRESSION] Invalid interval: {interval}")
                return jsonify({"error": "Invalid interval"})

            interval_id = row["interval_id"]


            sql = """
                SELECT
                    close_time,
                    close_price
                FROM fact_kline k
                JOIN dim_symbol s
                    ON k.symbol_id = s.symbol_id
                WHERE
                    s.symbol_name = %s
                    AND k.interval_id = %s
                ORDER BY close_time DESC
                LIMIT 50
            """

            cur.execute(sql, (symbol, interval_id))
            rows = cur.fetchall()

            if len(rows) < 5:
                return jsonify({"error": "Not enough data"})

            rows = rows[::-1]

            x = list(range(len(rows)))
            y = [float(r["close_price"]) for r in rows]

            import numpy as np

            X = np.array(x)
            Y = np.array(y)

            slope, intercept = np.polyfit(X, Y, 1)

            Y_pred = slope * X + intercept

            ss_res = np.sum((Y - Y_pred) ** 2)
            ss_tot = np.sum((Y - np.mean(Y)) ** 2)

            r2 = 1 - (ss_res / ss_tot)

            std_error = np.sqrt(ss_res / len(Y))

            return jsonify({

                "symbol": symbol,
                "interval": interval,

                "points": [
                    {"x": i, "y": y[i]}
                    for i in range(len(y))
                ],

                "line": Y_pred.tolist(),

                "slope": round(float(slope), 6),
                "r2": round(float(r2), 4),
                "std_error": round(float(std_error), 4)

            })

    except Exception as e:
        print("Regression API error:", e)
        return jsonify({"error": "Server error"})

    finally:
        if conn:
            conn.close()

@app.route("/api/analytics/stability")
def api_stability():

    symbol = request.args.get("symbol", "BTCUSDT")
    days = int(request.args.get("days", 14))  # số ngày test

    conn = pymysql.connect(**DB_CONFIG)

    with conn.cursor() as cur:
        cur.execute("""
            WITH base AS (
                SELECT
                    p.predicting_at,
                    CASE
                        WHEN r.result_status = 'WIN' THEN 1
                        ELSE 0
                    END AS is_win
                FROM fact_prediction p
                JOIN fact_prediction_result r
                    ON p.id = r.prediction_id
                JOIN dim_symbol s
                    ON p.symbol_id = s.symbol_id
                WHERE
                    s.symbol_name = %s
                    AND r.result_status IN ('WIN','LOSS')
                    AND p.predicting_at >= NOW() - INTERVAL %s DAY
            ),

            rolling AS (
                SELECT
                    predicting_at,

                    AVG(is_win) OVER (
                        ORDER BY predicting_at
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) * 100 AS win_rate_5,

                    AVG(is_win) OVER (
                        ORDER BY predicting_at
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                    ) * 100 AS win_rate_10,

                    AVG(is_win) OVER (
                        ORDER BY predicting_at
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) * 100 AS win_rate_20

                FROM base
            )

            SELECT
                DATE(predicting_at) AS d,
                ROUND(AVG(win_rate_5), 2)  AS win_rate_5,
                ROUND(AVG(win_rate_10), 2) AS win_rate_10,
                ROUND(AVG(win_rate_20), 2) AS win_rate_20
            FROM rolling
            GROUP BY d
            ORDER BY d;
        """, (symbol, days))

        rows = cur.fetchall()

    conn.close()

    if not rows:
        return jsonify({"error": "No data"})

    labels = [str(r["d"]) for r in rows]
    r5  = [float(r["win_rate_5"]) for r in rows]
    r10 = [float(r["win_rate_10"]) for r in rows]
    r20 = [float(r["win_rate_20"]) for r in rows]

    return jsonify({
        "symbol": symbol,     

        "labels": labels,
        "rolling_5": r5,
        "rolling_10": r10,
        "rolling_20": r20,

        "std_5": round(float(np.std(r5)), 2),
        "std_10": round(float(np.std(r10)), 2),
        "std_20": round(float(np.std(r20)), 2)
    })



@app.route("/api/analytics/edge-contribution")
def api_edge_contribution():

    conn = pymysql.connect(**DB_CONFIG)

    sql = """
        SELECT
            JSON_UNQUOTE(r.antecedent) AS metric_name,
            ROUND((r.lift - 1) * 100, 2) AS rule_strength,
            ROUND(r.lift, 2) AS lift,
            ROUND(r.confidence * 100, 2) AS win_rate
        FROM fp_growth_win_rules r
        WHERE r.confidence >= 0.6
        ORDER BY rule_strength DESC
        LIMIT 10;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    conn.close()

    return jsonify({
        "labels": [r["metric_name"] for r in rows],
        "edge": [float(r["rule_strength"]) for r in rows],
        "win_rate": [float(r["win_rate"]) for r in rows],
        "lift": [float(r["lift"]) for r in rows]
    })


@app.route("/api/analytics/expectancy")
def api_expectancy():

    symbol = request.args.get("symbol", "BTCUSDT")
    window = int(request.args.get("window", 20))  # 🔧 rolling trades
    conn = pymysql.connect(**DB_CONFIG)

    sql = f"""
        WITH base AS (
            SELECT
                p.predicting_at,
                r.pnl_pct,
                CASE WHEN r.result_status='WIN' THEN 1 ELSE 0 END AS is_win
            FROM fact_prediction p
            JOIN fact_prediction_result r
                ON p.id = r.prediction_id
            JOIN dim_symbol s
                ON p.symbol_id = s.symbol_id
            WHERE
                s.symbol_name = %s
                AND r.result_status IN ('WIN','LOSS')
        ),

        rolling AS (
            SELECT
                predicting_at,

                AVG(is_win) OVER w AS win_rate,

                AVG(CASE WHEN pnl_pct > 0 THEN pnl_pct END) OVER w AS avg_win,

                ABS(
                    AVG(CASE WHEN pnl_pct < 0 THEN pnl_pct END) OVER w
                ) AS avg_loss

            FROM base
            WINDOW w AS (
                ORDER BY predicting_at
                ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
            )
        )

        SELECT
            DATE(predicting_at) AS d,
            ROUND(win_rate * avg_win - (1 - win_rate) * avg_loss, 4) AS expectancy
        FROM rolling
        WHERE avg_win IS NOT NULL AND avg_loss IS NOT NULL
        ORDER BY d
    """

    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        rows = cur.fetchall()

    conn.close()

    return jsonify({
        "labels": [str(r["d"]) for r in rows],
        "expectancy": [float(r["expectancy"]) for r in rows]
    })

# @app.route("/api/analytics/sentiment-regime")
# def api_sentiment_regime():

#     symbol = request.args.get("symbol", "BTCUSDT")
#     conn = pymysql.connect(**DB_CONFIG)

#     sql = """
#         SELECT
#             regime,
#             COUNT(*) AS trades,
#             ROUND(
#                 SUM(CASE WHEN r.result_status = 'WIN' THEN 1 ELSE 0 END)
#                 / NULLIF(COUNT(*), 0) * 100,
#                 2
#             ) AS win_rate
#         FROM (
#             SELECT
#                 p.id,
#                 CASE
#                     WHEN p.metric_trend > 0 THEN 'TREND_UP'
#                     ELSE 'TREND_DOWN'
#                 END AS regime
#             FROM fact_prediction p

#             UNION ALL

#             SELECT
#                 p.id,
#                 CASE
#                     WHEN p.metric_momentum > 0 THEN 'MOMENTUM_POS'
#                     ELSE 'MOMENTUM_NEG'
#                 END
#             FROM fact_prediction p

#             UNION ALL

#             SELECT
#                 p.id,
#                 CASE
#                     WHEN p.metric_volatility > 0 THEN 'VOLATILITY_HIGH'
#                     ELSE 'VOLATILITY_LOW'
#                 END
#             FROM fact_prediction p

#             UNION ALL

#             SELECT
#                 p.id,
#                 CASE
#                     WHEN p.market_score >= 3 THEN 'EDGE_STRONG'
#                     ELSE 'EDGE_WEAK'
#                 END
#             FROM fact_prediction p
#         ) x
#         JOIN fact_prediction_result r
#             ON x.id = r.prediction_id
#         JOIN dim_symbol s
#             ON s.symbol_id = (
#                 SELECT symbol_id FROM fact_prediction WHERE id = x.id LIMIT 1
#             )
#         WHERE
#             s.symbol_name = %s
#             AND r.result_status IN ('WIN','LOSS')
#         GROUP BY regime
#         ORDER BY regime;

#     """

#     with conn.cursor() as cur:
#         cur.execute(sql, (symbol,))
#         rows = cur.fetchall()

#     conn.close()

#     return jsonify({
#         "labels": [r["regime"] for r in rows],
#         "win_rate": [float(r["win_rate"]) for r in rows],
#         "trades": [int(r["trades"]) for r in rows]
#     })

@app.route("/api/analytics/sentiment-regime")
def api_sentiment_regime():

    symbol = request.args.get("symbol", "BTCUSDT")
    conn = pymysql.connect(**DB_CONFIG)

    sql = """
        SELECT
            regime,
            COUNT(*) AS trades,
            ROUND(
                SUM(CASE WHEN r.result_status = 'WIN' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0) * 100,
                2
            ) AS win_rate
        FROM (
            SELECT
                p.id,
                CASE
                    WHEN p.metric_trend > 0 THEN 'TREND_UP'
                    ELSE 'TREND_DOWN'
                END AS regime
            FROM fact_prediction p

            UNION ALL

            SELECT
                p.id,
                CASE
                    WHEN p.metric_momentum > 0 THEN 'MOMENTUM_POS'
                    ELSE 'MOMENTUM_NEG'
                END
            FROM fact_prediction p

            UNION ALL

            SELECT
                p.id,
                CASE
                    WHEN p.metric_volatility > 0 THEN 'VOLATILITY_HIGH'
                    ELSE 'VOLATILITY_LOW'
                END
            FROM fact_prediction p

            UNION ALL

            SELECT
                p.id,
                CASE
                    WHEN p.market_score >= 3 THEN 'EDGE_STRONG'
                    ELSE 'EDGE_WEAK'
                END
            FROM fact_prediction p
        ) x
        JOIN fact_prediction_result r
            ON x.id = r.prediction_id
        JOIN fact_prediction p
            ON p.id = x.id
        JOIN dim_symbol s
            ON s.symbol_id = p.symbol_id
        WHERE
            s.symbol_name = %s
            AND r.result_status IN ('WIN','LOSS')
        GROUP BY regime
        HAVING COUNT(*) >= 30
        ORDER BY trades DESC
        LIMIT 5;
    """

    with conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        rows = cur.fetchall()

    conn.close()

    # ---- TOOLTIP TRADING LANGUAGE ----
    TOOLTIP_MAP = {
        "TREND_UP": "Uptrend regime – trend-following bias (BUY continuation favored)",
        "TREND_DOWN": "Downtrend regime – short / SELL continuation favored",
        "MOMENTUM_POS": "Positive momentum – breakout or continuation trades",
        "MOMENTUM_NEG": "Negative momentum – pullback or short bias",
        "VOLATILITY_HIGH": "High volatility – risk high, suitable for scalping / SELL spikes",
        "VOLATILITY_LOW": "Low volatility – range-bound, mean reversion setups",
        "EDGE_STRONG": "Strong edge – strategy historically performs well",
        "EDGE_WEAK": "Weak edge – avoid aggressive entries"
    }

    return jsonify({
        "labels": [r["regime"] for r in rows],
        "win_rate": [float(r["win_rate"]) for r in rows],
        "trades": [int(r["trades"]) for r in rows],
        "tooltips": [TOOLTIP_MAP.get(r["regime"], "") for r in rows]
    })

# @app.route("/api/analytics/equity-curve")
# def api_equity_curve():
#     symbol = request.args.get("symbol", "BTCUSDT")
#     conn = pymysql.connect(**DB_CONFIG)

#     SQL = """
#     SELECT
#         p.predicting_at,
#         r.pnl_pct
#     FROM fact_prediction p
#     JOIN fact_prediction_result r ON p.id = r.prediction_id
#     JOIN dim_symbol s ON p.symbol_id = s.symbol_id
#     WHERE
#         s.symbol_name = %s
#         AND r.result_status IN ('WIN','LOSS')
#     ORDER BY p.predicting_at
#     """

#     with conn.cursor(pymysql.cursors.DictCursor) as cur:
#         cur.execute(SQL, (symbol,))
#         rows = cur.fetchall()

#     conn.close()

#     equity = []
#     drawdown = []
#     peak = 0
#     capital = 100.0

#     for r in rows:
#         capital *= (1 + float(r["pnl_pct"]) / 100)
#         peak = max(peak, capital)
#         dd = (capital - peak) / peak * 100 if peak > 0 else 0

#         equity.append(round(capital, 2))
#         drawdown.append(round(dd, 2))

#     max_dd = round(min(drawdown), 2) if drawdown else 0

#     return jsonify({
#         "labels": [str(r["predicting_at"]) for r in rows],
#         "equity": equity,
#         "drawdown": drawdown,
#         "max_drawdown": max_dd
#     })

@app.route("/api/analytics/equity-curve")
def api_equity_curve():
    symbol = request.args.get("symbol", "BTCUSDT")
    conn = pymysql.connect(**DB_CONFIG)

    SQL = """
    SELECT
        p.predicting_at,
        r.pnl_pct
    FROM fact_prediction p
    JOIN fact_prediction_result r ON p.id = r.prediction_id
    JOIN dim_symbol s ON p.symbol_id = s.symbol_id
    WHERE
        s.symbol_name = %s
        AND r.result_status IN ('WIN','LOSS')
    ORDER BY p.predicting_at
    """

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(SQL, (symbol,))
        rows = cur.fetchall()

    conn.close()

    equity = []
    drawdown = []

    capital = 100.0
    peak = capital

    for r in rows:
        pnl = float(r["pnl_pct"]) / 100.0
        capital *= (1 + pnl)

        peak = max(peak, capital)
        dd = (capital - peak) / peak * 100  # ❗ giữ nguyên precision

        equity.append(round(capital, 2))     # UI
        drawdown.append(round(dd, 4))         # UI (đừng dùng 2)

    max_dd = round(min(drawdown), 4) if drawdown else 0

    return jsonify({
        "labels": [str(r["predicting_at"]) for r in rows],
        "equity": equity,
        "drawdown": drawdown,
        "max_drawdown": max_dd
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)


# @app.route("/api/analytics/correlations")
# def api_fp_correlations():

#     symbol = request.args.get("symbol", "BTCUSDT")

#     conn = pymysql.connect(**DB_CONFIG)

#     with conn.cursor() as cur:

#         cur.execute("""
#             SELECT
#                 m.metric_name   AS pattern,
#                 m.frequency_pct AS support,
#                 m.frequency_norm AS confidence

#             FROM metric_frequency_win m

#             JOIN dim_symbol s
#                 ON m.symbol_id = s.symbol_id

#             WHERE
#                 s.symbol_name = %s

#             ORDER BY
#                 m.frequency_norm DESC,
#                 m.frequency_pct DESC

#             LIMIT 8
#         """, (symbol,))

#         rows = cur.fetchall()

#     conn.close()

#     labels = []
#     values = []

#     for r in rows:

#         short = r["pattern"][:25]

#         labels.append(short)
#         values.append(float(r["confidence"]))

#     return jsonify({
#         "labels": labels,
#         "values": values
#     })



# @app.route("/api/analytics/performance")
# def api_performance():

#     symbol = request.args.get("symbol", "BTCUSDT")
#     interval = request.args.get("interval", "2H")

#     # Hiện tại chỉ dùng 2H
#     interval_map = {
#         "2H": 1
#     }

#     interval_id = interval_map.get(interval)

#     if not interval_id:
#         return jsonify({"error": "Invalid interval"})

#     conn = pymysql.connect(**DB_CONFIG)

#     try:
#         with conn.cursor() as cur:

#             sql = """
#                 SELECT
#                     DATE(p.signal_time) AS d,

#                     -- FP: Có trade (BUY/SELL)
#                     AVG(
#                         CASE
#                             WHEN p.signal_type != 'SIDEWAY' THEN 1
#                             ELSE 0
#                         END
#                     ) AS fp_rate,

#                     -- REG: BUY signal
#                     AVG(
#                         CASE
#                             WHEN p.signal_type = 'BUY' THEN 1
#                             ELSE 0
#                         END
#                     ) AS reg_rate

#                 FROM fact_prediction p

#                 JOIN dim_symbol s
#                     ON p.symbol_id = s.symbol_id

#                 WHERE
#                     s.symbol_name = %s
#                     AND p.interval_id = %s

#                 GROUP BY d
#                 ORDER BY d DESC
#                 LIMIT 30;

#             """

#             cur.execute(sql, (symbol, interval_id))

#             rows = cur.fetchall()

#             labels = []
#             fp = []
#             reg = []

#             # Đảo lại để vẽ từ cũ → mới
#             rows = rows[::-1]

#             for r in rows:

#                 labels.append(str(r["d"]))

#                 fp.append(round((r["fp_rate"] or 0) * 100, 2))
#                 reg.append(round((r["reg_rate"] or 0) * 100, 2))

#             return jsonify({
#                 "labels": labels,
#                 "fp": fp,
#                 "reg": reg
#             })

#     except Exception as e:

#         print("Performance API error:", e)

#         return jsonify({"error": "Server error"})

#     finally:

#         conn.close()


# @app.route("/api/analytics/news-impact")
# def api_news_impact():

#     symbol = request.args.get("symbol", "BTCUSDT")

#     conn = pymysql.connect(**DB_CONFIG)

#     try:
#         with conn.cursor() as cur:

#             sql = """
#                 SELECT
#                     CASE
#                         WHEN n.symbol_id IS NULL THEN 'NO_NEWS'
#                         ELSE 'HAS_NEWS'
#                     END AS news_group,

#                     COUNT(*) AS total_trades,

#                     ROUND(
#                         SUM(CASE WHEN r.result_status = 'WIN' THEN 1 ELSE 0 END)
#                         /
#                         NULLIF(
#                             SUM(CASE WHEN r.result_status IN ('WIN','LOSS') THEN 1 ELSE 0 END),
#                             0
#                         ) * 100,
#                         2
#                     ) AS win_rate

#                 FROM fact_prediction p

#                 JOIN fact_prediction_result r
#                     ON p.id = r.prediction_id

#                 JOIN dim_symbol s
#                     ON p.symbol_id = s.symbol_id

#                 LEFT JOIN news_sentiment_agg_fact n
#                     ON p.symbol_id = n.symbol_id
#                    AND p.signal_time > n.window_start
#                    AND p.signal_time <= n.window_end

#                 WHERE
#                     s.symbol_name = %s
#                     AND r.result_status IN ('WIN','LOSS')

#                 GROUP BY news_group
#                 ORDER BY news_group DESC
#             """

#             cur.execute(sql, (symbol,))
#             rows = cur.fetchall()

#         labels = []
#         values = []
#         counts = []

#         for r in rows:
#             labels.append(r["news_group"])
#             values.append(float(r["win_rate"] or 0))
#             counts.append(int(r["total_trades"] or 0))

#         return jsonify({
#             "labels": labels,
#             "values": values,
#             "counts": counts
#         })

#     except Exception as e:
#         print("NEWS IMPACT API ERROR:", e)
#         return jsonify({})

#     finally:
#         conn.close()