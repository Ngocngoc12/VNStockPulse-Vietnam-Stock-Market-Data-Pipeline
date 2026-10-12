from datetime import datetime, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="vnstockpulse_check",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["vnstockpulse", "diagnostics"],
)
def vnstockpulse_check():
    @task()
    def inspect_runtime():
        import json
        import shutil
        import sys
        from pathlib import Path
        from importlib.util import find_spec

        info = {
            "python": sys.version.split()[0],
            "java": shutil.which("java"),
            "spark_submit": shutil.which("spark-submit"),
            "pyspark_installed": find_spec("pyspark") is not None,
            "spark_jobs": {
                name: (Path("/opt/airflow/spark_jobs") / name).is_file()
                for name in [
                    "bronze_minio.py",
                    "silver_minio.py",
                    "gold_minio.py",
                ]
            },
        }
        print("RUNTIME CHECK:")
        print(json.dumps(info, indent=2))
        return info

    @task()
    def check_minio():
        from urllib.request import urlopen

        with urlopen("http://minio:9000/minio/health/ready", timeout=15) as response:
            if response.status != 200:
                raise RuntimeError(f"MinIO returned HTTP {response.status}")
        print("MINIO READY: HTTP 200")
        return {"minio_ready": True}

    inspect_runtime() >> check_minio()


vnstockpulse_check()