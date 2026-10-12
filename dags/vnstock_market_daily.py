import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
CRAWLER = "/opt/airflow/ingestion/crawl_ohlcv_incremental.py"


@dag(
    dag_id="vnstock_market_daily",
    description="Cap nhat OHLCV sau khi thi truong dong cua",
    schedule="30 15 * * 1-5",
    start_date=pendulum.datetime(2026, 10, 1, tz=VN_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "vnstockpulse",
        "retries": 3,
        "retry_delay": timedelta(minutes=20),
    },
    tags=["vnstock", "ohlcv", "daily", "minio"],
)
def vnstock_market_daily():
    @task(execution_timeout=timedelta(hours=2))
    def crawl_ohlcv_incremental():
        command = [
            sys.executable,
            "-u",
            CRAWLER,
        ]

        print("Running:", " ".join(command))

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None

        for line in process.stdout:
            print(line, end="", flush=True)

        return_code = process.wait()

        if return_code != 0:
            raise RuntimeError(
                f"Incremental crawler failed with exit code {return_code}"
            )

        return {
            "status": "completed",
            "schedule": "15:30 Asia/Ho_Chi_Minh",
        }

    crawl_ohlcv_incremental()


vnstock_market_daily()