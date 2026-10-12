import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_ROOT = PROJECT_ROOT / "data" / "serving" / "gold"
SCHEMA_FILE = PROJECT_ROOT / "contracts" / "ohlcv.avsc"

TOPIC = "vnstock.ohlcv.demo"
BOOTSTRAP_SERVERS = "127.0.0.1:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
MESSAGE_LIMIT = 100


def find_latest_batch() -> Path:
    batches = sorted(
        [
            path
            for path in GOLD_ROOT.glob("batch_id=*")
            if path.is_dir()
            and (path / "manifest.json").is_file()
            and list((path / "latest").glob("*.parquet"))
        ],
        key=lambda path: path.name,
        reverse=True,
    )

    if not batches:
        raise FileNotFoundError(
            f"Khong tim thay Gold export hoan chinh trong {GOLD_ROOT}"
        )

    return batches[0]


def required_number(value, field_name: str) -> float:
    number = float(value)

    if not math.isfinite(number):
        raise ValueError(f"{field_name} khong hop le: {value}")

    return number


def row_to_event(row: dict) -> dict:
    event_time = pd.to_datetime(row["time"]).date().isoformat()

    ingested_value = (
        row.get("gold_created_at")
        or row.get("ingested_at")
        or datetime.now(timezone.utc)
    )
    ingested_at = pd.to_datetime(ingested_value, utc=True).isoformat()

    return {
        "symbol": str(row["symbol"]).strip().upper(),
        "time": event_time,
        "open": required_number(row["open"], "open"),
        "high": required_number(row["high"], "high"),
        "low": required_number(row["low"], "low"),
        "close": required_number(row["close"], "close"),
        "volume": int(row["volume"]),
        "source": str(row["source"]).strip().upper(),
        "ingested_at": ingested_at,
    }


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"FAILED: {error}")
        return

    print(
        f"DELIVERED | topic={message.topic()} "
        f"partition={message.partition()} offset={message.offset()}"
    )
def main() -> None:
    batch_dir = find_latest_batch()
    latest_dir = batch_dir / "latest"
    parquet_files = sorted(latest_dir.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"Khong tim thay file Parquet trong {latest_dir}"
        )

    frame = pd.concat(
        [pd.read_parquet(file) for file in parquet_files],
        ignore_index=True,
    ).head(MESSAGE_LIMIT)

    if frame.empty:
        raise RuntimeError("Gold latest khong co du lieu")

    schema_text = SCHEMA_FILE.read_text(encoding="utf-8")
    schema_client = SchemaRegistryClient(
        {"url": SCHEMA_REGISTRY_URL}
    )

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_client,
        schema_str=schema_text,
        to_dict=lambda event, context: event,
        conf={"auto.register.schemas": True},
    )

    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "broker.address.family": "v4",
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "acks": "all",
        }
    )

    sent = 0

    for row in frame.to_dict(orient="records"):
        event = row_to_event(row)

        producer.produce(
            topic=TOPIC,
            key=event["symbol"],
            value=event,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        sent += 1

    remaining = producer.flush(30)

    if remaining != 0:
        raise RuntimeError(
            f"Con {remaining} message chua gui duoc"
        )

    print("=" * 70)
    print("KAFKA PRODUCER COMPLETED")
    print(f"Batch    : {batch_dir.name}")
    print(f"Topic    : {TOPIC}")
    print(f"Messages : {sent}")
    print("=" * 70)


if __name__ == "__main__":
    main()