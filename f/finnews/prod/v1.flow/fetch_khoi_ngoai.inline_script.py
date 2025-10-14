import requests
import time


def fetch_with_retry(
    url: str, params: dict = None, max_retries: int = 3, timeout: int = 30
):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} for {url}")
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if not data or "data" not in data:
                if attempt < max_retries - 1:
                    print(f"Empty data received, retrying...")
                    time.sleep(2**attempt)
                    continue
                else:
                    return None

            return data
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
            else:
                return None
    return None


def main(stock_market: str, base_url: str = "http://172.18.0.10:8000"):
    KEY_FORWARD_COMPATIBLE = {"HSX": "VNINDEX", "HNX": "HNXINDEX", "UPCOM": "UPINDEX"}

    data = fetch_with_retry(f"{base_url}/khoi_ngoai", params={"period": "1W"})

    if not data:
        return {
            "khoi_ngoai": {"vol": 0, "net_value": 0},
            "success": False,
            "error": "Failed to fetch data after 3 retries",
        }

    stock_market = stock_market.upper() if stock_market else "HSX"
    stock_market = KEY_FORWARD_COMPATIBLE.get(stock_market, stock_market)

    if stock_market not in data["data"]:
        return {
            "khoi_ngoai": {"vol": 0, "net_value": 0},
            "success": False,
            "error": f"Market {stock_market} not found",
        }

    market_data = data["data"][stock_market].get("data", {})
    vol = market_data.get("tradingVolumeChart_first", {}).get("value", 0) / 10**6
    net_value = market_data.get("tradingValueChart_first", {}).get("value", 0) / 10**9

    return {
        "khoi_ngoai": {"vol": vol, "net_value": net_value},
        "success": True,
        "data_quality": {
            "market_used": stock_market,
            "volume_millions": vol,
            "value_billions": net_value,
        },
    }
