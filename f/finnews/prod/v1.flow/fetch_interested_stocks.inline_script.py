import requests
import time


def fetch_with_retry(url: str, max_retries: int = 3, timeout: int = 30):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} for {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            if not data or "data" not in data or not data["data"]:
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


def main(base_url: str = "http://172.18.0.10:8000"):
    data = fetch_with_retry(f"{base_url}/top_interested_stocks")

    if not data:
        return {
            "top_interested": [],
            "success": False,
            "error": "Failed to fetch data after 3 retries",
        }

    top_stocks = [e.get("symbol", "Unknown") for e in data["data"]][:10]
    return {
        "top_interested": top_stocks,
        "success": True,
        "data_quality": {
            "records_processed": len(top_stocks),
            "source_records": len(data["data"]),
        },
    }
