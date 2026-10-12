import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio


def main():
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")
    username = os.getenv("MINIO_ROOT_USER")
    password = os.getenv("MINIO_ROOT_PASSWORD")
    if not username or not password:
        raise ValueError("Missing MINIO_ROOT_USER or MINIO_ROOT_PASSWORD")

    raw_dir = root / "data" / "raw"
    files = sorted(p for p in raw_dir.rglob("*.json") if p.is_file())
    if not files:
        raise FileNotFoundError(f"No JSON files found in {raw_dir}")

    client = Minio(
        "localhost:9000",
        access_key=username,
        secret_key=password,
        secure=False,
    )
    bucket = "vnstockpulse"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    for index, path in enumerate(files, start=1):
        key = "raw/" + path.relative_to(raw_dir).as_posix()
        client.fput_object(bucket, key, str(path),
                           content_type="application/json")
        uploaded = client.stat_object(bucket, key)
        if uploaded.size != path.stat().st_size:
            raise RuntimeError(f"Size mismatch: {key}")
        print(f"[{index}/{len(files)}] Verified: {key}")

    print(f"Completed: {len(files)} files -> s3://{bucket}/raw/")


if __name__ == "__main__":
    main()