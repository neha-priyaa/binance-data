# download.py

import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

# -------------------------
# helpers
# -------------------------

def generate_months(start, end):
    """returns ['YYYY-MM', ...]"""
    months = []
    cur = start
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        cur += relativedelta(months=1)
    return months


def generate_url(symbol, interval, period):
    """builds single zip url"""
    return f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{period}.zip"


# -------------------------
# core downloader
# -------------------------

def download_data(symbols, interval, start_year=2025, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    periods = generate_months(start, end)

    for symbol in symbols:
        pair_dir = os.path.join("data", symbol)

        if not os.path.exists(pair_dir):
            os.makedirs(pair_dir)

        for period in periods:
            url = generate_url(symbol, interval, period)
            file_path = os.path.join(
                pair_dir, f"{symbol}-{interval}-{period}.zip"
            )

            if os.path.exists(file_path):
                print(f"Already exists: {file_path}")
                continue

            print(f"Downloading {url}")
            r = requests.get(url, stream=True)

            if r.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            else:
                print(f"Failed: {url} ({r.status_code})")


# -------------------------
# run
# -------------------------

if __name__ == "__main__":
    pairs = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT"
    ]

    download_data(
        symbols=pairs,
        interval="1h",
        start_year=2025,
        end_year=2025
    )

