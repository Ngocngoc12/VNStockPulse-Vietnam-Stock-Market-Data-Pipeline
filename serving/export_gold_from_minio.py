import json
import os
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio import Minio

BATCH_ID = "20261008_182058_4b127219"
BUCKET = "vnstockpulse"
PREFIX = f"gold/ohlcv_features/batch_id={BATCH_ID}/"


def main():
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")
    username = os.getenv("MINIO_ROOT_USER")
    password = os.getenv("MINIO_ROOT_PASSWORD")
    if not username or not password:
        raise ValueError("Missing MinIO credentials in .env")
    client = Minio("localhost:9000", access_key=username,
                   secret_key=password, secure=False)
    parent = root / "data" / "serving" / "gold"
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / f"batch_id={BATCH_ID}"
    if target.exists():
        raise FileExistsError(f"Export already exists: {target}")

    expected = {"history": 3330522, "latest": 1521}
    counts = {}
    staging = Path(tempfile.mkdtemp(prefix=".export_", dir=parent))
    for kind, expected_rows in expected.items():
        prefix = PREFIX + kind + "/"
        client.stat_object(BUCKET, prefix + "_SUCCESS")
        folder = staging / kind
        folder.mkdir()
        rows = 0
        for obj in client.list_objects(BUCKET, prefix=prefix, recursive=True):
            if not obj.object_name.endswith(".parquet"):
                continue
            relative = obj.object_name[len(prefix):]
            local = (folder / relative).resolve()
            if not local.is_relative_to(folder.resolve()):
                raise ValueError("Invalid object path")
            local.parent.mkdir(parents=True, exist_ok=True)
            client.fget_object(BUCKET, obj.object_name, str(local))
            if local.stat().st_size != obj.size:
                raise RuntimeError(f"Size mismatch: {relative}")
            rows += pq.ParquetFile(local).metadata.num_rows
        if rows != expected_rows:
            raise RuntimeError(f"{kind}: expected {expected_rows}, got {rows}")
        counts[kind] = rows
        print(f"Verified {kind}: {rows:,} rows")

    manifest = {"batch_id": BATCH_ID, "source": PREFIX, "rows": counts}
    (staging / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    staging.rename(target)
    print(f"EXPORT COMPLETED\nOutput: {target}")


if __name__ == "__main__":
    main()