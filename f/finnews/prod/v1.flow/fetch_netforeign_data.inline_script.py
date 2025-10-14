import requests
import time


def fetch_with_retry(url: str, max_retries: int = 3, timeout: int = 30):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} for {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if not data or ("top_buy" not in data and "top_sell" not in data):
                if attempt < max_retries - 1:
                    print(f"Invalid data received, retrying...")
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


def process_netforeign_data(data, limit=10):
    if not data:
        return {"top_netforeign": {"buy": [], "sell": []}}

    sorted_buy = sorted(
        data.get("top_buy", []), key=lambda x: x.get("value", 0), reverse=True
    )
    sorted_sell = sorted(data.get("top_sell", []), key=lambda x: x.get("value", 0))

    return {
        "top_netforeign": {
            "buy": [item.get("ticker", "Unknown") for item in sorted_buy[:limit]],
            "sell": [item.get("ticker", "Unknown") for item in sorted_sell[:limit]],
        }
    }


def main(base_url: str = "http://172.18.0.10:8000"):
    data = fetch_with_retry(f"{base_url}/top_netforeign")

    if not data:
        return {
            "top_netforeign": {"buy": [], "sell": []},
            "success": False,
            "error": "Failed to fetch data after 3 retries",
        }

    processed_data = process_netforeign_data(data)
    return {
        **processed_data,
        "success": True,
        "data_quality": {
            "buy_records": len(processed_data["top_netforeign"]["buy"]),
            "sell_records": len(processed_data["top_netforeign"]["sell"]),
        },
    }
