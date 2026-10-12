"""
DAG: vnstock_daily_ingestion
- Tầng thu thập (Ingestion layer) trong kiến trúc vnstockpulse.
- Gọi trực tiếp crawl_all_stocks_daily() từ ingestion/vnstock/crawl_ohlcv_daily.py
  (import Python, KHÔNG dùng subprocess) -> hàm này tự crawl toàn bộ mã và trả
  về list kết quả dạng:
      [{"symbol": "VNM", "status": "SUCCESS", "output_file": "data/raw/ohlcv_daily/crawl_ohlcv_daily_VNM_2026_09_08.json"},
       {"symbol": "ABC", "status": "FAILED", "error": "..."}]
- Task thứ 2 đọc từng output_file của các mã SUCCESS, publish từng record lên Kafka.

Đặt file này vào: vnstockpulse/dags/

LƯU Ý QUAN TRỌNG trước khi chạy trong Docker:
1. crawl_ohlcv_daily.py dùng ĐƯỜNG DẪN TƯƠNG ĐỐI ("data/raw/...", "data/checkpoint")
   -> phụ thuộc vào working directory lúc Airflow worker chạy task, KHÔNG phải lúc nào
   cũng là /opt/airflow/ingestion/vnstock. Cách an toàn nhất: os.chdir() vào đúng thư
   mục ingestion/vnstock TRƯỚC khi gọi hàm crawl (đã xử lý bên dưới bằng biến
   INGESTION_WORKDIR), và nhớ mount thêm volume để "data/" không bị mất khi container
   restart (xem docker-compose.yaml, cần thêm volume cho ./ingestion/vnstock/data).
2. VNSTOCK_API_KEY đang hardcode trong crawl_ohlcv_daily.py -> nên chuyển thành
   os.environ.get("VNSTOCK_API_KEY") và set qua docker-compose environment, để không
   commit key lên git.
"""

from __future__ import annotations
from datetime import timedelta

import pendulum
import json
import os
import sys
from datetime import datetime, timedelta
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import dag, task  # Airflow 3.x TaskFlow API

# Thư mục chứa crawl_ohlcv_daily.py bên trong container Airflow.
# Phải chdir vào đây trước khi gọi hàm, vì script dùng path tương đối "data/raw/...".
INGESTION_WORKDIR = "/opt/airflow/ingestion/vnstock"
KAFKA_TOPIC = "vnstock.ohlcv.daily"

default_args = {
    "owner": "vnstockpulse",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
@dag(
    dag_id="vnstock_daily_ingestion",
    schedule=CronTriggerTimetable(
        "30 15 * * 1-5",
        timezone="Asia/Ho_Chi_Minh",
    ),
    start_date=pendulum.datetime(
        2025,
        1,
        1,
        tz="Asia/Ho_Chi_Minh",
    ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["ingestion", "vnstock", "kafka"],
)
def vnstock_daily_ingestion():

    @task
    def crawl_ohlcv() -> list[dict]:
        """
        Import trực tiếp crawl_all_stocks_daily() -- không subprocess.
        Trả về list kết quả (symbol/status/output_file hoặc error) để
        task sau biết chính xác file nào cần đọc, mã nào bị lỗi.
        """
        os.chdir(INGESTION_WORKDIR)
        if INGESTION_WORKDIR not in sys.path:
            sys.path.insert(0, INGESTION_WORKDIR)

        from crawl_ohlcv_daily import crawl_all_stocks_daily

        results = crawl_all_stocks_daily()
        return results

    @task
    def publish_to_kafka(results: list[dict]) -> dict:
        """
        Đọc output_file của từng mã SUCCESS, publish từng record lên Kafka
        (Avro + Schema Registry) để tầng Bronze tiêu thụ.
        Mã FAILED không có output_file -> bỏ qua, log lại để theo dõi
        (đây chính là chỗ nên nối vào Dead Letter Queue ở Data Contract layer sau này).
        """
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer

        schema_registry_conf = {"url": os.environ["SCHEMA_REGISTRY_URL"]}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Schema Avro tối thiểu, khớp field thật script tạo ra (symbol, source,
        # collected_at + các cột OHLCV do vnstock trả về, tên cột có thể khác nhau
        # tuỳ nguồn "VCI" -- nên set permissive hơn bằng cách serialize cả record
        # dưới dạng string JSON nếu bạn chưa chốt schema chuẩn. Ở đây giả định các
        # cột chuẩn: time, open, high, low, close, volume.
        value_schema_str = """
        {
          "type": "record",
          "name": "OhlcvRecord",
          "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "source", "type": "string"},
            {"name": "time", "type": ["null", "string"], "default": null},
            {"name": "open", "type": ["null", "double"], "default": null},
            {"name": "high", "type": ["null", "double"], "default": null},
            {"name": "low", "type": ["null", "double"], "default": null},
            {"name": "close", "type": ["null", "double"], "default": null},
            {"name": "volume", "type": ["null", "long"], "default": null},
            {"name": "collected_at", "type": "string"},
            {"name": "ingested_at", "type": "string"}
          ]
        }
        """

        avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)
        producer_conf = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "value.serializer": avro_serializer,
        }
        producer = SerializingProducer(producer_conf)

        now_iso = datetime.utcnow().isoformat()
        published = 0
        skipped_failed_symbols = []

        for entry in results:
            if entry.get("status") != "SUCCESS":
                skipped_failed_symbols.append(entry.get("symbol"))
                continue

            output_file = entry["output_file"]
            with open(output_file, "r", encoding="utf-8") as f:
                records = json.load(f)

            for rec in records:
                rec["ingested_at"] = now_iso
                # Chỉ giữ field có trong schema để serializer không lỗi vì field lạ
                clean_rec = {
                    k: rec.get(k)
                    for k in (
                        "symbol", "source", "time", "open", "high",
                        "low", "close", "volume", "collected_at", "ingested_at",
                    )
                }
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=clean_rec["symbol"],
                    value=clean_rec,
                )
                published += 1

        producer.flush()

        if skipped_failed_symbols:
            print(f"Bỏ qua {len(skipped_failed_symbols)} mã crawl lỗi: {skipped_failed_symbols}")

        return {"published": published, "failed_symbols": skipped_failed_symbols}

    results = crawl_ohlcv()
    publish_to_kafka(results)


vnstock_daily_ingestion()