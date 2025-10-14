import requests
import time


def fetch_with_retry(
    url: str, params: dict = None, max_retries: int = 3, timeout: int = 90
):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} for {url}")
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                if attempt < max_retries - 1:
                    print(f"API error received, retrying...")
                    time.sleep(2**attempt)
                    continue
                else:
                    return data

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
    stock_market = stock_market if stock_market else "HSX"

    data = fetch_with_retry(
        f"{base_url}/index_fluctuation", params={"index_name": stock_market}
    )

    if not data:
        return {
            "impact_up": {"stock_code": [], "total": 0},
            "impact_down": {"stock_code": [], "total": 0},
            "success": False,
            "error": "Failed to fetch data after 3 retries",
        }

    if "error" in data:
        return {
            "impact_up": {"stock_code": [], "total": 0},
            "impact_down": {"stock_code": [], "total": 0},
            "success": False,
            "error": data["error"],
        }

    index_increase = []
    index_decrease = []

    for item in data["data"]:
        impact = item.get("index_affect", 0)
        if impact >= 0:
            index_increase.append(item)
        else:
            index_decrease.append(item)

    return {
        "impact_up": {
            "stock_code": [e.get("ticker", "Unknown") for e in index_increase],
            "total": sum([e.get("index_affect", 0) for e in index_increase]),
        },
        "impact_down": {
            "stock_code": [e.get("ticker", "Unknown") for e in index_decrease],
            "total": sum([e.get("index_affect", 0) for e in index_decrease]),
        },
        "success": True,
        "data_quality": {
            "increase_count": len(index_increase),
            "decrease_count": len(index_decrease),
            "market_used": stock_market,
        },
    }
