import os, io, json, gzip, boto3
from typing import Iterable, List

BUCKET = os.getenv("S3_BUCKET", "zcw-students-projects")
BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "data/tracktionai/").rstrip("/") + "/"
REGION = os.getenv("AWS_REGION", "us-east-1")

def _key(k: str) -> str:
    return BASE_PREFIX + k.lstrip("/")

def client():
    return boto3.client("s3", region_name=REGION)  # uses AWS CLI profile/role

def list_keys(prefix: str) -> List[str]:
    c, keys, token = client(), [], None
    while True:
        params = {"Bucket": BUCKET, "Prefix": _key(prefix)}
        if token: params["ContinuationToken"] = token
        resp = c.list_objects_v2(**params)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        token = resp.get("NextContinuationToken")
        if not token: break
    return keys

def put_text(key: str, text: str):
    client().put_object(Bucket=BUCKET, Key=_key(key), Body=text.encode(), ContentType="text/plain")

def get_bytes(key: str) -> bytes:
    return client().get_object(Bucket=BUCKET, Key=_key(key))["Body"].read()