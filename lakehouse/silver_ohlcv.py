from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "ohlcv"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "ohlcv"
DEAD_LETTER_ROOT = PROJECT_ROOT / "data" / "dead_letter"

REQUIRED_COLUMNS = [
    "symbol",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
]


def get_latest_bronze_batch() -> Path:
    batches = sorted(
        [p for p in BRONZE_ROOT.glob("batch_id=*") if p.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )

    if not batches:
        raise FileNotFoundError("Không tìm thấy Bronze batch trong data/bronze/ohlcv")

    return batches[0]


def read_bronze_batch(batch_dir: Path) -> pd.DataFrame:
    files = sorted(batch_dir.glob("part-*.parquet"))

    if not files:
        raise FileNotFoundError(f"Không tìm thấy file parquet trong {batch_dir}")

    frames = [pd.read_parquet(file) for file in files]
    return pd.concat(frames, ignore_index=True)


def clean_ohlcv(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    original_rows = len(df)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Thiếu cột bắt buộc: {col}")

    df = df.copy()

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["source"] = df["source"].astype(str).str.strip().str.upper()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    invalid_mask = (
        df["symbol"].isna()
        | (df["symbol"] == "")
        | df["time"].isna()
        | df["close"].isna()
    )

    invalid_df = df[invalid_mask].copy()
    clean_df = df[~invalid_mask].copy()

    before_dedup = len(clean_df)

    clean_df = clean_df.drop_duplicates(
        subset=["symbol", "time", "source"],
        keep="last",
    )

    clean_df = clean_df.sort_values(["symbol", "time"]).reset_index(drop=True)

    print("=" * 70)
    print("Silver cleaning summary")
    print("=" * 70)
    print(f"Original rows      : {original_rows:,}")
    print(f"Invalid rows       : {len(invalid_df):,}")
    print(f"Before deduplicate : {before_dedup:,}")
    print(f"After deduplicate  : {len(clean_df):,}")
    print(f"Duplicates removed : {before_dedup - len(clean_df):,}")

    return clean_df, invalid_df


def write_invalid_records(invalid_df: pd.DataFrame, batch_id: str) -> None:
    if invalid_df.empty:
        return

    DEAD_LETTER_ROOT.mkdir(parents=True, exist_ok=True)
    output_file = DEAD_LETTER_ROOT / f"ohlcv_silver_invalid_{batch_id}.parquet"
    invalid_df.to_parquet(output_file, index=False)

    print(f"Invalid records saved: {output_file}")


def main() -> None:
    bronze_batch_dir = get_latest_bronze_batch()
    batch_id = bronze_batch_dir.name.replace("batch_id=", "")

    print("=" * 70)
    print("VNStockPulse - Silver OHLCV")
    print("=" * 70)
    print(f"Bronze batch: {batch_id}")

    bronze_df = read_bronze_batch(bronze_batch_dir)
    silver_df, invalid_df = clean_ohlcv(bronze_df)

    output_dir = SILVER_ROOT / f"batch_id={batch_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "ohlcv_silver.parquet"
    silver_df.to_parquet(output_file, index=False)

    write_invalid_records(invalid_df, batch_id)

    print("=" * 70)
    print("Silver OHLCV completed")
    print("=" * 70)
    print(f"Rows written: {len(silver_df):,}")
    print(f"Output      : {output_file}")


if __name__ == "__main__":
    main()