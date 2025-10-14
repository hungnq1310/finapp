import re
from enum import Enum
from datetime import datetime


class StockMarket(Enum):
    HSX = "HSX"
    HNX = "HNX"
    UPCOM = "UPCOM"


def validate_stock_market(market: str) -> str:
    if not market:
        return "HSX"
    try:
        return StockMarket(market.upper()).value
    except ValueError:
        print(f"Invalid stock market: {market}, defaulting to HSX")
        return "HSX"


def validate_minio_config(
    endpoint: str, bucket: str, access_key: str, secret_key: str
) -> dict:
    errors = []

    if not endpoint:
        errors.append("MinIO endpoint is required")
    if not bucket or len(bucket) < 3:
        errors.append("MinIO bucket name is required and must be at least 3 characters")
    if not access_key:
        errors.append("MinIO access key is required")
    if not secret_key:
        errors.append("MinIO secret key is required")

    if bucket and not re.match(r"^[a-z0-9.-]{3,63}$", bucket):
        errors.append("Invalid bucket name format")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "validated_config": {
            "endpoint": endpoint,
            "bucket": bucket,
            "access_key": access_key,
            "secret_key": secret_key,
        },
    }


def main(
    stock_market: str,
    minio_endpoint: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str,
):
    validated_market = validate_stock_market(stock_market)
    minio_validation = validate_minio_config(
        minio_endpoint, minio_bucket, minio_access_key, minio_secret_key
    )

    if not minio_validation["valid"]:
        raise ValueError(
            f"Configuration errors: {', '.join(minio_validation['errors'])}"
        )

    return {
        "stock_market": validated_market,
        "minio_config": minio_validation["validated_config"],
        "validation_passed": True,
        "timestamp": datetime.now().isoformat(),
    }
