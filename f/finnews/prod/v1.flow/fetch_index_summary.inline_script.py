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
    data = fetch_with_retry(f"{base_url}/index_summary")

    if not data:
        return {
            "index_summary": [],
            "success": False,
            "error": "Failed to fetch data after 3 retries",
        }

    key_fields = [
        "indexId",
        "indexValue",
        "change",
        "changePercent",
        "allQty",
        "allValue",
        "advances",
        "nochanges",
        "declines",
    ]

    result_dict = []
    for item in data["data"]:
        temp_dict = {}
        for key in key_fields:
            temp_dict[key] = item.get(key, "N/A")
        result_dict.append(temp_dict)

    return {
        "index_summary": result_dict,
        "success": True,
        "data_quality": {
            "records_processed": len(result_dict),
            "source_records": len(data["data"]),
        },
    }
