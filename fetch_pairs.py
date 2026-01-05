
import requests

BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"


def fetch_usdt_pairs():
    """
    Fetch all active USDT spot trading pairs from Binance
    """
    response = requests.get(BINANCE_EXCHANGE_INFO, timeout=10)
    response.raise_for_status()

    data = response.json()

    pairs = [
        s["symbol"]
        for s in data["symbols"]
        if s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
        and s["isSpotTradingAllowed"]
    ]

    return pairs


if __name__ == "__main__":
    pairs = fetch_usdt_pairs()
    print(f"Total USDT pairs: {len(pairs)}")
    print(pairs[:20])

