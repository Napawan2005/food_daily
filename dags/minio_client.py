import os
import boto3
import pandas as pd

def get_minio_client(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

def list_minio_buckets(client) -> list[str]:
    return [b["Name"] for b in client.list_buckets()["Buckets"]]



