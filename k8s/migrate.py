"""Copy S3 objects from the local RustFS and verify their SHA-256."""

import argparse
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from botocore.config import Config

from hh_analytics.utils import sha256


def client(endpoint, prefix):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ[f"{prefix}_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5,
        ),
    )


def copy_buckets(source, target):
    existing = {item["Name"] for item in target.list_buckets()["Buckets"]}
    paginator = source.get_paginator("list_objects_v2")

    with TemporaryDirectory() as directory:
        local = str(Path(directory) / "object")
        copied = str(Path(directory) / "verified")

        for bucket in source.list_buckets()["Buckets"]:
            name = bucket["Name"]
            if name not in existing:
                target.create_bucket(Bucket=name)

            for page in paginator.paginate(Bucket=name):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    meta = source.head_object(Bucket=name, Key=key)
                    source.download_file(name, key, local)
                    target.upload_file(
                        local,
                        name,
                        key,
                        ExtraArgs={
                            "ContentType": meta["ContentType"],
                            "Metadata": meta["Metadata"],
                        },
                    )
                    target.download_file(name, key, copied)
                    expected = sha256(local)
                    if expected != sha256(copied):
                        raise ValueError(f"SHA-256 отличается: {name}/{key}")

                    print(
                        json.dumps(
                            {
                                "bucket": name,
                                "key": key,
                                "bytes": meta["ContentLength"],
                                "sha256": expected,
                                "verified": True,
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source")
    parser.add_argument("target")
    args = parser.parse_args()
    copy_buckets(client(args.source, "SOURCE_S3"), client(args.target, "S3"))


if __name__ == "__main__":
    main()
