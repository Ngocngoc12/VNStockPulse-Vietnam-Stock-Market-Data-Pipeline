import argparse
import hashlib
import json
import os
import re
import time
from datetime import date, datetime, time as dt_time, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

import pandas as pd
from minio import Minio
from minio.error import S3Error

os.environ.setdefault("VNSTOCK_TELEMETRY", "off")
import sys
from pathlib import Path
# This project already has ingestion/vnstock, which would shadow
# the installed vnstock package when this script runs directly.
INGESTION_DIR = Path(__file__).resolve().parent

sys.path = [
    entry
    for entry in sys.path
    if Path(entry or ".").resolve() != INGESTION_DIR
]
from vnstock import Market


VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

BUCKET = os.getenv("MINIO_BUCKET", "vnstockpulse")
BACKFILL_PREFIX = "raw/ohlcv_backfill/"
DAILY_PREFIX = "raw/ohlcv_daily"
DEAD_LETTER_PREFIX = "dead_letter/ohlcv_daily"
STATE_OBJECT = "_state/ohlcv_daily.json"

DEFAULT_START_DATE = os.getenv(
    "OHLCV_INITIAL_START_DATE",
    "2026-09-09",
)

SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")
REQUEST_DELAY = float(os.getenv("VNSTOCK_REQUEST_DELAY", "0.5"))
MAX_ERROR_RATIO = float(os.getenv("VNSTOCK_MAX_ERROR_RATIO", "0.10"))


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def put_json(
    client: Minio,
    object_name: str,
    payload: object,
) -> None:
    content = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    ).encode("utf-8")

    client.put_object(
        bucket_name=BUCKET,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type="application/json",
    )


def read_state(client: Minio) -> dict:
    try:
        response = client.get_object(BUCKET, STATE_OBJECT)

        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

    except S3Error as error:
        if error.code in {"NoSuchKey", "NoSuchObject"}:
            return {}

        raise


def load_symbols(client: Minio) -> list[str]:
    pattern = re.compile(
        r"crawl_ohlcv_backfill_(.+)_\d{4}_\d{2}_\d{2}\.json$"
    )

    symbols = set()

    for item in client.list_objects(
        BUCKET,
        prefix=BACKFILL_PREFIX,
        recursive=True,
    ):
        match = pattern.search(item.object_name)

        if match:
            symbols.add(match.group(1).upper())

    if not symbols:
        raise RuntimeError(
            f"No symbols found under s3://{BUCKET}/{BACKFILL_PREFIX}"
        )

    return sorted(symbols)


def latest_completed_market_date() -> date:
    now = datetime.now(VN_TZ)
    target = now.date()

    if now.time() < dt_time(15, 30):
        target -= timedelta(days=1)

    while target.weekday() >= 5:
        target -= timedelta(days=1)

    return target


def resolve_date_range(
    client: Minio,
    start_date: str | None,
    end_date: str | None,
) -> tuple[date, date]:
    state = read_state(client)

    if start_date:
        start = date.fromisoformat(start_date)
    elif state.get("last_successful_date"):
        start = (
            date.fromisoformat(state["last_successful_date"])
            + timedelta(days=1)
        )
    else:
        start = date.fromisoformat(DEFAULT_START_DATE)

    end = (
        date.fromisoformat(end_date)
        if end_date
        else latest_completed_market_date()
    )

    return start, end


def prepare_frame(
    frame: pd.DataFrame,
    start: date,
    end: date,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()

    frame = frame.copy().reset_index()
    frame.columns = [
        str(column).strip().lower()
        for column in frame.columns
    ]

    if "time" not in frame.columns:
        for candidate in ("date", "trading_date", "index"):
            if candidate in frame.columns:
                frame = frame.rename(columns={candidate: "time"})
                break

    required = {
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }

    missing = required.difference(frame.columns)

    if missing:
        raise ValueError(
            f"Missing VNStock columns: {sorted(missing)}"
        )

    frame["time"] = pd.to_datetime(
        frame["time"],
        errors="coerce",
    )

    frame = frame.dropna(
        subset=[
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    )

    frame["trading_date"] = frame["time"].dt.date

    return frame[
        (frame["trading_date"] >= start)
        & (frame["trading_date"] <= end)
    ]


def run(
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
    update_state: bool = True,
) -> dict:
    client = get_minio_client()

    if not client.bucket_exists(BUCKET):
        raise RuntimeError(f"MinIO bucket does not exist: {BUCKET}")

    start, end = resolve_date_range(
        client,
        start_date,
        end_date,
    )

    if start > end:
        result = {
            "status": "no_new_date",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "rows_written": 0,
        }
        print(json.dumps(result, indent=2))
        return result

    symbols = load_symbols(client)

    if limit:
        symbols = symbols[:limit]

    now = datetime.now(VN_TZ)
    collected_at = now.isoformat()
    run_id = now.strftime("%Y%m%d_%H%M%S")

    # VNStock treats end as an exclusive boundary.
    api_end = end + timedelta(days=1)

    market = Market()
    errors = []
    rows_written = 0
    symbols_with_data = 0

    print("=" * 70)
    print("VNStock incremental OHLCV")
    print(f"Symbols : {len(symbols)}")
    print(f"Range   : {start} -> {end}")
    print("=" * 70)

    for position, symbol in enumerate(symbols, start=1):
        try:
            frame = market.equity(symbol).ohlcv(
                start=start.isoformat(),
                end=api_end.isoformat(),
                interval="1D",
            )

            frame = prepare_frame(frame, start, end)

            if not frame.empty:
                symbols_with_data += 1

            for _, row in frame.iterrows():
                trading_date = row["trading_date"].isoformat()

                business_data = {
                    "symbol": symbol,
                    "time": trading_date,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(float(row["volume"])),
                    "source": SOURCE,
                }

                fingerprint = hashlib.sha256(
                    json.dumps(
                        business_data,
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest()[:16]

                payload = {
                    **business_data,
                    "collected_at": collected_at,
                }

                object_name = (
                    f"{DAILY_PREFIX}/"
                    f"trading_date={trading_date}/"
                    f"symbol={symbol}/"
                    f"part-{fingerprint}.json"
                )

                put_json(client, object_name, payload)
                rows_written += 1

        except Exception as error:
            errors.append(
                {
                    "symbol": symbol,
                    "error": str(error),
                }
            )

        if position % 50 == 0 or position == len(symbols):
            print(
                f"[{position}/{len(symbols)}] "
                f"rows={rows_written} errors={len(errors)}"
            )

        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

    error_ratio = (
        len(errors) / len(symbols)
        if symbols
        else 0
    )

    if errors:
        put_json(
            client,
            (
                f"{DEAD_LETTER_PREFIX}/"
                f"run_id={run_id}/errors.json"
            ),
            errors,
        )

    result = {
        "status": "completed",
        "run_id": run_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "symbol_count": len(symbols),
        "symbols_with_data": symbols_with_data,
        "rows_written": rows_written,
        "error_count": len(errors),
        "error_ratio": round(error_ratio, 4),
    }

    if error_ratio > MAX_ERROR_RATIO:
        raise RuntimeError(
            "Too many VNStock errors: "
            + json.dumps(result)
        )

    if update_state:
        put_json(
            client,
            STATE_OBJECT,
            {
                "last_successful_date": end.isoformat(),
                "updated_at": collected_at,
                "last_run": result,
            },
        )

    print("=" * 70)
    print("INCREMENTAL OHLCV COMPLETED")
    print(json.dumps(result, indent=2))
    print("=" * 70)

    return result


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--no-update-state",
        action="store_true",
    )

    args = parser.parse_args()

    run(
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        update_state=not args.no_update_state,
    )


if __name__ == "__main__":
    main()