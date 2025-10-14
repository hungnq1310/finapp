import requests
import time


def fetch_with_retry(url: str, max_retries: int = 3, timeout: int = 30):
    """Fetch data with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} for {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            # Check if data is valid
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


def sort_and_format_sectors(data, limit=5):
    if not data or "data" not in data:
        return []
    sorted_data = sorted(
        data["data"], key=lambda x: x.get("changePercent", 0), reverse=True
    )
    for item in sorted_data:
        item["changePercent"] = f"{item.get('changePercent', 0) * 100:.2f}%"
    return sorted_data[:limit]


def main(base_url: str = "http://172.18.0.10:8000"):
    data = fetch_with_retry(f"{base_url}/top_sectors")

    if not data:
        return {
            "top_sectors": [],
            "success": False,
            "error": "Failed to fetch data after 3 retries",
        }

    processed_data = sort_and_format_sectors(data)
    return {
        "top_sectors": [e.get("icbName", "Unknown") for e in processed_data],
        "success": True,
        "data_quality": {
            "records_processed": len(processed_data),
            "source_records": len(data.get("data", [])),
        },
    }
