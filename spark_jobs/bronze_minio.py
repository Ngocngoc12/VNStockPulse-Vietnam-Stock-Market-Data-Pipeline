from datetime import datetime, timezone
from uuid import uuid4
from pyspark.sql import SparkSession, functions as F

batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
batch_id += "_" + uuid4().hex[:8]
source = "s3a://vnstockpulse/raw/ohlcv_backfill/crawl_ohlcv_backfill_*.json"
output = f"s3a://vnstockpulse/bronze/ohlcv/batch_id={batch_id}"

spark = (
    SparkSession.builder.appName("VNStockPulse-Bronze-MinIO")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    raw = (spark.read.option("multiLine", "true")
           .option("mode", "FAILFAST").json(source))
    print(f"Input files: {len(raw.inputFiles())}")
    bronze = (
        raw.withColumn("raw_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )
    bronze.repartition(8).write.mode("errorifexists").parquet(output)
    rows = spark.read.parquet(output).count()
    if rows == 0:
        raise RuntimeError("Bronze output is empty")
    print(f"BRONZE COMPLETED | Batch: {batch_id}")
    print(f"Rows read back: {rows:,}")
    print(f"Output: {output}")
finally:
    spark.stop()