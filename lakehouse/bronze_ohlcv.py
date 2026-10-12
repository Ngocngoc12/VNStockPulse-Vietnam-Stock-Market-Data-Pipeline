import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIRS = [
    PROJECT_ROOT / "data" / "raw" / "ohlcv_backfill",
    PROJECT_ROOT / "data" / "raw" / "ohlcv_daily",
]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "ohlcv"
DEAD_LETTER_DIR = PROJECT_ROOT / "data" / "dead_letter"

CORE_COLUMNS = [
    "symbol",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "collected_at",
]


def make_batch_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def make_record_hash(row: dict) -> str:
    """
    Hash dùng để nhận diện cùng một record.
    Bronze chưa xóa duplicate, chỉ gắn hash để tầng Silver xử lý.
    """
    key = "|".join(
        str(row.get(col, "")) for col in ["symbol", "time", "source", "open", "high", "low", "close", "volume"]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def load_json_file(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        return [data]

    raise ValueError(f"Unsupported JSON format: {path}")


def normalize_records(records: list[dict], raw_file: Path, batch_id: str) -> list[dict]:
    normalized = []
    ingested_at = datetime.now().isoformat(timespec="seconds")

    for rec in records:
        row = {}

        for col in CORE_COLUMNS:
            row[col] = rec.get(col)

        row["ingested_at"] = ingested_at
        row["batch_id"] = batch_id
        row["raw_file"] = str(raw_file.relative_to(PROJECT_ROOT))
        row["record_hash"] = make_record_hash(row)

        normalized.append(row)

    return normalized


def write_dead_letter(errors: list[dict], batch_id: str) -> None:
    if not errors:
        return

    DEAD_LETTER_DIR.mkdir(parents=True, exist_ok=True)
    output_file = DEAD_LETTER_DIR / f"ohlcv_bronze_errors_{batch_id}.json"

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    print(f"Dead letter saved: {output_file}")


def iter_raw_files() -> list[Path]:
    files = []

    for raw_dir in RAW_DIRS:
        if not raw_dir.exists():
            continue

        files.extend(sorted(raw_dir.glob("*.json")))

    return files


def main() -> None:
    batch_id = make_batch_id()
    output_dir = BRONZE_DIR / f"batch_id={batch_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_files = iter_raw_files()

    if not raw_files:
        raise FileNotFoundError(
            "Không tìm thấy file raw OHLCV trong data/raw/ohlcv_backfill hoặc data/raw/ohlcv_daily"
        )

    print("=" * 70)
    print("VNStockPulse - Bronze OHLCV")
    print("=" * 70)
    print(f"Batch ID : {batch_id}")
    print(f"Raw files: {len(raw_files)}")

    errors = []
    buffer = []
    part_no = 0
    rows_written = 0

    # Ghi từng phần để tránh ôm toàn bộ 1500+ file vào RAM.
    files_per_part = 100

    for idx, raw_file in enumerate(raw_files, start=1):
        try:
            records = load_json_file(raw_file)
            normalized = normalize_records(records, raw_file, batch_id)
            buffer.extend(normalized)
        except Exception as e:
            errors.append(
                {
                    "raw_file": str(raw_file),
                    "error": str(e),
                    "failed_at": datetime.now().isoformat(timespec="seconds"),
                }
            )

        should_flush = len(buffer) > 0 and idx % files_per_part == 0
        is_last_file = idx == len(raw_files)

        if should_flush or is_last_file:
            if buffer:
                df = pd.DataFrame(buffer)

                output_file = output_dir / f"part-{part_no:05d}.parquet"
                df.to_parquet(output_file, index=False)

                rows_written += len(df)
                print(f"Written {len(df):,} rows -> {output_file}")

                buffer = []
                part_no += 1

    write_dead_letter(errors, batch_id)

    print("=" * 70)
    print("Bronze OHLCV completed")
    print("=" * 70)
    print(f"Batch ID     : {batch_id}")
    print(f"Rows written : {rows_written:,}")
    print(f"Parts written: {part_no}")
    print(f"Errors       : {len(errors)}")
    print(f"Output       : {output_dir}")


if __name__ == "__main__":
    main()