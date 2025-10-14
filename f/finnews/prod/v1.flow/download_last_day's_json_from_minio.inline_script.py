import boto3
import json
import io
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

MAPPING_KEY = {
    "VN30": "VN30",
    "HNX30": "HNX30",
    "UPCOM": "HNXUpcomIndex",
    "VNXALL": "VNXALL",
    "VNINDEX": "VNINDEX",
    "HNXINDEX": "HNXIndex",
}


def get_last_trading_day():
    """
    Tính toán ngày giao dịch gần nhất:
    - Nếu hôm nay là Thứ 2 (weekday = 0), lấy dữ liệu Thứ 6 tuần trước
    - Các ngày khác thì lấy dữ liệu ngày hôm trước
    """
    today = datetime.now()

    if today.weekday() == 0:  # Monday
        # Lấy dữ liệu Thứ 6 tuần trước (3 ngày trước)
        last_trading_day = today - timedelta(days=3)
        print(
            f"Hôm nay là Thứ 2, sẽ lấy dữ liệu Thứ 6: {last_trading_day.strftime('%Y-%m-%d')}"
        )
    else:
        # Lấy dữ liệu ngày hôm trước
        last_trading_day = today - timedelta(days=1)
        print(f"Lấy dữ liệu ngày trước: {last_trading_day.strftime('%Y-%m-%d')}")

    return last_trading_day


def main(minio_config: dict):
    """
    Downloads last trading day's JSON report from MinIO and extracts
    the 'allValue' (GTGD in billions) and 'allQty' (KLGD in millions) for each index.
    """
    # Create MinIO client (S3-compatible)
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_config.get("endpoint"),
        aws_access_key_id=minio_config.get("access_key"),
        aws_secret_access_key=minio_config.get("secret_key"),
        config=boto3.session.Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Tính ngày giao dịch gần nhất
    last_trading_day = get_last_trading_day()

    # Generate target filename pattern
    date_str = last_trading_day.strftime("%Y%m%d")
    bucket = minio_config.get("bucket")

    try:
        # List objects to find the most recent file from last trading day
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=f"stock_report_{date_str}"
        )

        if "Contents" not in response or len(response["Contents"]) == 0:
            return {
                "success": True,
                "last_day_gtdg": {},
                "last_day_klgd": {},
                "message": f"No JSON file found for {date_str}",
                "trading_date": last_trading_day.strftime("%Y-%m-%d"),
            }

        # Get the most recent file (sorted by LastModified)
        files = sorted(
            response["Contents"], key=lambda x: x["LastModified"], reverse=True
        )
        target_key = files[0]["Key"]

        print(f"Attempting to download last trading day's report from: {target_key}")

        # Download JSON file
        obj_response = s3_client.get_object(Bucket=bucket, Key=target_key)
        json_content = obj_response["Body"].read().decode("utf-8")
        data = json.loads(json_content)

        last_day_gtdg = {}
        last_day_klgd = {}

        # Extract index_summary from JSON structure
        index_summary = []

        # Handle different possible JSON structures
        if "data" in data and "index_summary" in data["data"]:
            index_summary = data["data"]["index_summary"]
        elif "index_summary" in data:
            index_summary = data["index_summary"]
        else:
            return {
                "success": False,
                "last_day_gtdg": {},
                "last_day_klgd": {},
                "error": "Invalid JSON structure - index_summary not found",
                "trading_date": last_trading_day.strftime("%Y-%m-%d"),
            }

        # Process index_summary data
        for item in index_summary:
            index_id = item.get("indexId", "")

            # Check if this index is in our mapping
            if index_id in MAPPING_KEY.values() or index_id in MAPPING_KEY.keys():
                # Use the mapped name or original
                mapped_index_name = index_id

                # Convert GTGD (allValue) to billions if not already
                gtdg_value = item.get("allValue", 0)
                if gtdg_value != "N/A" and gtdg_value is not None:
                    try:
                        # If value is already in billions (from previous processing)
                        if (
                            gtdg_value < 10000
                        ):  # Assume < 10000 means already in billions
                            last_day_gtdg[mapped_index_name] = float(gtdg_value)
                        else:  # Convert from base units to billions
                            last_day_gtdg[mapped_index_name] = float(gtdg_value) / 10**9
                    except (ValueError, TypeError) as e:
                        print(f"Error processing GTGD for {index_id}: {e}")

                # Convert KLGD (allQty) to millions if not already
                klgd_value = item.get("allQty", 0)
                if klgd_value != "N/A" and klgd_value is not None:
                    try:
                        # If value is already in millions
                        if (
                            klgd_value < 100000
                        ):  # Assume < 100000 means already in millions
                            last_day_klgd[mapped_index_name] = float(klgd_value)
                        else:  # Convert from base units to millions
                            last_day_klgd[mapped_index_name] = float(klgd_value) / 10**6
                    except (ValueError, TypeError) as e:
                        print(f"Error processing KLGD for {index_id}: {e}")

        print(f"Successfully extracted data from {target_key}")
        print(f"GTGD data: {last_day_gtdg}")
        print(f"KLGD data: {last_day_klgd}")

        return {
            "success": True,
            "last_day_gtdg": last_day_gtdg,
            "last_day_klgd": last_day_klgd,
            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
            "source_file": target_key,
        }

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {
                "success": True,
                "last_day_gtdg": {},
                "last_day_klgd": {},
                "message": f"No JSON file found for trading day {date_str}",
                "trading_date": last_trading_day.strftime("%Y-%m-%d"),
            }
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": str(e),
            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
        }
    except Exception as e:
        return {
            "success": False,
            "last_day_gtdg": {},
            "last_day_klgd": {},
            "error": f"Unexpected error processing JSON: {e}",
            "trading_date": last_trading_day.strftime("%Y-%m-%d"),
        }
