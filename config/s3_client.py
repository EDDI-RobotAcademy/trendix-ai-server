import os
import boto3

AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def build_s3_url(key: str) -> str:
    if AWS_REGION:
        return f"https://{AWS_S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"
    return f"https://{AWS_S3_BUCKET}.s3.amazonaws.com/{key}"
