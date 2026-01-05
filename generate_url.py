BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

def generate_url(pair, timeframe, period):
    return f"{BASE_URL}/{pair}/{timeframe}/{pair}-{timeframe}-{period}.zip"


# run it
url = generate_url("BTCUSDT", "1h", "2023-11")
print(url)

