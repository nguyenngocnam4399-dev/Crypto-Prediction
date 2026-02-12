from kafka import KafkaProducer
import requests
import json
import time
import os
from datetime import datetime
from typing import Dict

# =========================
# KAFKA CONFIG (PRODUCTION SAFE)
# =========================
BROKER = "localhost:9092"
TOPIC = "crypto-prediction"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    request_timeout_ms=30000
)

# =========================
# BINANCE CONFIG
# =========================
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

# 🔒 FIXED: each interval must be a single valid Binance interval
INTERVALS = ["2h"]

LIMIT = 5000
SLEEP_SEC = 10
BINANCE_URL = "https://api.binance.com/api/v3/klines"

# =====================================================
# HTTP SESSION (REUSE CONNECTION)
# =====================================================
session = requests.Session()
session.headers.update({
    "User-Agent": "CryptoKafkaProducer/1.0"
})

# =========================
# STATE MANAGEMENT
# =========================
STATE_FILE = "/root/crypto_ubuntu/producer_state.json"


def load_state() -> Dict[str, int]:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state: Dict[str, int]):
    tmp_file = STATE_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump(state, f)
    os.replace(tmp_file, STATE_FILE)  # atomic write


# =========================
# BINANCE FETCH
# =========================
def fetch_klines(symbol, interval, limit):
    res = requests.get(
        BINANCE_URL,
        params={
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        },
        timeout=10
    )
    res.raise_for_status()
    return res.json()

# =====================================================
# KAFKA CALLBACK
# =====================================================
def on_send_error(excp):
    print("=> Kafka send failed:", excp)

# =========================
# PRODUCER LOOP
# =========================
def produce_loop():
    last_timestamp = load_state()

    print("🚀 Binance Producer started (PRODUCTION MODE)")
    print(f"Symbols   : {SYMBOLS}")
    print(f"Intervals : {INTERVALS}")
    print(f"Limit     : {LIMIT}")
    print(f"Sleep     : {SLEEP_SEC}s")
    print("--------------------------------------------------")

    try:
        while True:
            for symbol in SYMBOLS:
                for interval in INTERVALS:
                    key = f"{symbol}-{interval}"
                    new_count = 0

                    try:
                        klines = fetch_klines(symbol, interval, LIMIT)
                    except Exception as e:
                        print(f"❌ Fetch error {key}: {e}")
                        continue

                    for k in klines[:-1]:
                        open_time = k[0]

                        # ===== DEDUP =====
                        if open_time <= last_timestamp.get(key, 0):
                            continue

                        msg = {
                            "symbol": symbol,
                            "interval": interval,
                            "open_time": open_time,
                            "open": float(k[1]),
                            "high": float(k[2]),
                            "low": float(k[3]),
                            "close": float(k[4]),
                            "volume": float(k[5]),
                            "close_time": k[6],
                            "fetched_at": datetime.utcnow().isoformat()
                        }

                        producer.send(TOPIC, key=key, value=msg).add_errback(on_send_error)
                        last_timestamp[key] = open_time
                        new_count += 1

                    # SAVE STATE AFTER EACH SYMBOL-INTERVAL
                    save_state(last_timestamp)

                    # ===== CLEAN & USEFUL LOG =====
                    if new_count > 0:
                        print(
                            f"[{datetime.utcnow()}] {key} | "
                            f"new_candles={new_count} | "
                            f"last_open_time={last_timestamp[key]}"
                        )
                    else:
                        print(
                            f"[{datetime.utcnow()}] {key} | no new candle"
                        )

            # FLUSH BATCH
            producer.flush(timeout=30)
            print("✅ Kafka flush completed")
            print("--------------------------------------------------")

            time.sleep(SLEEP_SEC)

    except KeyboardInterrupt:
        print("=> Producer stopped by user")

    finally:
        save_state(last_timestamp)
        producer.close()
        print("=> Producer closed safely")


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    produce_loop()
