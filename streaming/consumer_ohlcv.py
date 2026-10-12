import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw_streaming" / "ohlcv"

TOPIC = "vnstock.ohlcv.demo"
BOOTSTRAP_SERVERS = "127.0.0.1:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
MESSAGE_LIMIT = 100
IDLE_TIMEOUT_SECONDS = 20


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    schema_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_client)

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "broker.address.family": "v4",
            "group.id": f"vnstockpulse-demo-{uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
        }
    )

    consumer.subscribe([TOPIC])

    run_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = OUTPUT_DIR / f"ohlcv_events_{run_time}.jsonl"

    records = []
    last_message_at = time.monotonic()

    print(f"Listening to topic: {TOPIC}")

    try:
        while len(records) < MESSAGE_LIMIT:
            message = consumer.poll(1.0)

            if message is None:
                if time.monotonic() - last_message_at >= IDLE_TIMEOUT_SECONDS:
                    print("Khong nhan them message trong 20 giay.")
                    break
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            event = message.value()

            record = {
                **event,
                "_kafka": {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key(),
                },
            }

            records.append(record)
            last_message_at = time.monotonic()

            print(
                f"CONSUMED {len(records):03d} | "
                f"{event['symbol']} | {event['time']} | "
                f"close={event['close']}"
            )
    finally:
        consumer.close()

    if not records:
        raise RuntimeError("Consumer khong nhan duoc message nao")

    with output_file.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")

    print("=" * 70)
    print("KAFKA CONSUMER COMPLETED")
    print(f"Messages : {len(records)}")
    print(f"Output   : {output_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()