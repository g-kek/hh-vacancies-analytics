import csv
import io
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from .config import ROOT
from .utils import sha256

load_dotenv(ROOT / ".env")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def bucket():
    return os.getenv("S3_BUCKET", "hh-lake")


def create_bucket():
    client = s3_client()

    try:
        client.head_bucket(Bucket=bucket())
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
            raise

        client.create_bucket(Bucket=bucket())


def upload(path, key):
    path = Path(path)
    client = s3_client()
    client.upload_file(
        str(path),
        bucket(),
        key,
        ExtraArgs={"Metadata": {"sha256": sha256(path)}},
    )
    meta = client.head_object(Bucket=bucket(), Key=key)
    if meta["ContentLength"] != path.stat().st_size:
        raise AssertionError(f"Размер объекта {key} не совпал")
    return {
        "key": key,
        "bytes": meta["ContentLength"],
        "sha256": meta["Metadata"]["sha256"],
        "etag": meta["ETag"],
    }


def stream_csv(key):
    response = s3_client().get_object(Bucket=bucket(), Key=key)

    with io.TextIOWrapper(response["Body"], encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f)
