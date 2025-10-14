import boto3
import json
import tempfile
import os
from datetime import datetime
from botocore.exceptions import ClientError


def save_json_to_minio(
    data: dict,
    minio_endpoint: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> dict:
    """Save JSON data to MinIO"""
    try:
        # Create MinIO client (compatible with S3 API)
        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=boto3.session.Config(signature_version="s3v4"),
            region_name="us-east-1",  # MinIO doesn't require specific region
        )

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stock_report_{timestamp}.json"

        # Create temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as temp_file:
            json.dump(data, temp_file, ensure_ascii=False, indent=2)
            temp_file_path = temp_file.name

        # Upload to MinIO
        s3_client.upload_file(
            temp_file_path,
            minio_bucket,
            filename,
            ExtraArgs={"ContentType": "application/json"},
        )

        # Get file size
        file_size = os.path.getsize(temp_file_path)

        # Cleanup
        os.unlink(temp_file_path)

        # Generate presigned URL (optional, for download link)
        try:
            presigned_url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": minio_bucket, "Key": filename},
                ExpiresIn=86400,  # 24 hours
            )
        except:
            presigned_url = None

        return {
            "success": True,
            "message": "JSON saved to MinIO successfully",
            "minio_location": f"{minio_endpoint}/{minio_bucket}/{filename}",
            "filename": filename,
            "file_size_bytes": file_size,
            "presigned_url": presigned_url,
            "timestamp": datetime.now().isoformat(),
        }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        return {
            "success": False,
            "message": f"MinIO upload failed: {error_code}",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Unexpected error: {str(e)}",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }


def main(
    stock_market: str,
    data: dict,
    minio_endpoint: str,
    minio_bucket: str,
    minio_access_key: str,
    minio_secret_key: str,
):
    """Main function to save data to MinIO"""

    # Add metadata to the data before saving
    enhanced_data = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "stock_market": stock_market,
            "data_quality": data.pop("data_quality", {}),  # pop non-used
            "execution_metadata": data.pop("execution_metadata", {}),  # pop non-used
        },
        "data": data,  # only store required fields
    }

    result = save_json_to_minio(
        enhanced_data, minio_endpoint, minio_bucket, minio_access_key, minio_secret_key
    )

    # Print summary
    if result["success"]:
        print(f"✓ JSON saved successfully to MinIO")
        print(f"  Location: {result['minio_location']}")
        print(f"  File size: {result['file_size_bytes']} bytes")
        if result.get("presigned_url"):
            print(f"  Download URL: {result['presigned_url']}")
    else:
        print(f"✗ Failed to save to MinIO: {result['message']}")

    return result
