import pandas as pd

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

def generate_batch_urls(pair, timeframe, from_date, to_date):
    periods = pd.date_range(
        start=from_date,
        end=to_date,
        freq="MS"
    ).strftime("%Y-%m")

    return [
        f"{BASE_URL}/{pair}/{timeframe}/{pair}-{timeframe}-{period}.zip"
        for period in periods
    ]


# run it
urls = generate_batch_urls(
    "BTCUSDT",
    "1h",
    "2025-01",
    "2025-10"
)

for u in urls:
    print(u)

