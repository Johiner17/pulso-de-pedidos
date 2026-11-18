"""Exercise a small S3 API workflow against the local AWS Emulator only."""

import os
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config


ENDPOINT = os.environ["AWS_ENDPOINT_URL"]
CREDENTIALS = Path(os.environ["AWS_SHARED_CREDENTIALS_FILE"])
CA_BUNDLE = Path(os.environ["AWS_CA_BUNDLE"])


def wait_for_emulator(timeout_seconds: int = 90) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if CREDENTIALS.is_file() and CA_BUNDLE.is_file():
            return
        time.sleep(1)
    raise TimeoutError("AWS Emulator no generó sus credenciales y certificado a tiempo")


def main() -> None:
    wait_for_emulator()
    s3 = boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=ENDPOINT,
        verify=str(CA_BUNDLE),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )

    bucket = f"pulso-cdc-smoke-{uuid.uuid4().hex[:12]}"
    key = "checks/s3-smoke.txt"
    body = b"Pulso de pedidos - prueba local de AWS Emulator\n"
    bucket_created = False

    try:
        s3.create_bucket(Bucket=bucket)
        bucket_created = True
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/plain")

        downloaded = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if downloaded != body:
            raise AssertionError("El contenido recuperado de S3 no coincide con el enviado")

        objects = s3.list_objects_v2(Bucket=bucket, Prefix="checks/")["Contents"]
        if not any(item["Key"] == key for item in objects):
            raise AssertionError("ListObjectsV2 no devolvió el objeto que se acaba de escribir")

        s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_bucket(Bucket=bucket)
        bucket_created = False
        print(
            "AWS Emulator S3 smoke: PASS "
            "(CreateBucket, PutObject, GetObject, ListObjectsV2, DeleteObject, DeleteBucket)"
        )
    finally:
        if bucket_created:
            s3.delete_object(Bucket=bucket, Key=key)
            s3.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
