from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "ohlcv"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold" / "ohlcv_features"


def get_latest_silver_batch() -> Path:
    batches = sorted(
        [p for p in SILVER_ROOT.glob("batch_id=*") if p.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )

    if not batches:
        raise FileNotFoundError("Không tìm thấy Silver batch trong data/silver/ohlcv")

    return batches[0]


def read_silver_batch(batch_dir: Path) -> pd.DataFrame:
    file = batch_dir / "ohlcv_silver.parquet"

    if not file.exists():
        raise FileNotFoundError(f"Không tìm thấy file Silver: {file}")

    return pd.read_parquet(file)


def add_rsi(group: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    delta = group["close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()

    rs = avg_gain / avg_loss
    group["rsi14"] = 100 - (100 / (1 + rs))

    return group


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.sort_values(["symbol", "time"]).reset_index(drop=True)

    result_frames = []

    for symbol, group in df.groupby("symbol", sort=False):
        group = group.copy()

        group["return_1d"] = group["close"].pct_change(1)
        group["return_5d"] = group["close"].pct_change(5)
        group["return_20d"] = group["close"].pct_change(20)

        group["ma20"] = group["close"].rolling(window=20, min_periods=20).mean()
        group["ma50"] = group["close"].rolling(window=50, min_periods=50).mean()

        group["volume_ma20"] = group["volume"].rolling(window=20, min_periods=20).mean()

        group = add_rsi(group, window=14)

        ema12 = group["close"].ewm(span=12, adjust=False).mean()
        ema26 = group["close"].ewm(span=26, adjust=False).mean()
        group["macd"] = ema12 - ema26
        group["macd_signal"] = group["macd"].ewm(span=9, adjust=False).mean()

        result_frames.append(group)

    features_df = pd.concat(result_frames, ignore_index=True)

    keep_columns = [
        "symbol",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "return_1d",
        "return_5d",
        "return_20d",
        "ma20",
        "ma50",
        "volume_ma20",
        "rsi14",
        "macd",
        "macd_signal",
        "batch_id",
        "ingested_at",
    ]

    existing_columns = [col for col in keep_columns if col in features_df.columns]
    features_df = features_df[existing_columns]

    return features_df


def main() -> None:
    silver_batch_dir = get_latest_silver_batch()
    batch_id = silver_batch_dir.name.replace("batch_id=", "")

    print("=" * 70)
    print("VNStockPulse - Gold OHLCV Features")
    print("=" * 70)
    print(f"Silver batch: {batch_id}")

    silver_df = read_silver_batch(silver_batch_dir)
    print(f"Silver rows : {len(silver_df):,}")

    features_df = add_features(silver_df)

    output_dir = GOLD_ROOT / f"batch_id={batch_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "ohlcv_features.parquet"
    features_df.to_parquet(output_file, index=False)

    latest_df = (
        features_df.sort_values(["symbol", "time"])
        .groupby("symbol", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    latest_file = output_dir / "ohlcv_latest_features.parquet"
    latest_df.to_parquet(latest_file, index=False)

    print("=" * 70)
    print("Gold OHLCV Features completed")
    print("=" * 70)
    print(f"Rows written       : {len(features_df):,}")
    print(f"Latest rows written: {len(latest_df):,}")
    print(f"Output             : {output_file}")
    print(f"Latest output      : {latest_file}")


if __name__ == "__main__":
    main()