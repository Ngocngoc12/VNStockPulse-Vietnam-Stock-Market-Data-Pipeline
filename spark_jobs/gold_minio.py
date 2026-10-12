from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BATCH_ID = "20261008_182058_4b127219"
BASE = "s3a://vnstockpulse"
SOURCE = f"{BASE}/silver/ohlcv/batch_id={BATCH_ID}"
OUTPUT = f"{BASE}/gold/ohlcv_features/batch_id={BATCH_ID}"


def main():
    spark = (
        SparkSession.builder.appName("VNStockPulse-Gold-MinIO")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = spark.read.parquet(SOURCE)
        window = Window.partitionBy("symbol", "source").orderBy("time")
        gold = (
            silver.withColumn("previous_valid_date", F.lag("time").over(window))
            .withColumn("previous_valid_close", F.lag("close").over(window))
            .withColumn("gap_calendar_days",
                        F.datediff("time", "previous_valid_date"))
            .withColumn("return_prev_valid",
                        F.col("close") / F.col("previous_valid_close") - 1)
        )
        for size in [20, 50]:
            rolling = window.rowsBetween(1 - size, 0)
            gold = gold.withColumn(
                f"ma{size}", F.when(F.count("close").over(rolling) == size,
                                   F.avg("close").over(rolling)))
        rolling20 = window.rowsBetween(-19, 0)
        gold = gold.withColumn(
            "volume_ma20", F.when(F.count("volume").over(rolling20) == 20,
                                 F.avg("volume").over(rolling20)))
        gold = gold.withColumn("gold_created_at", F.current_timestamp())
        gold.repartition(8).write.mode("errorifexists").parquet(f"{OUTPUT}/history")

        history = spark.read.parquet(f"{OUTPUT}/history")
        newest = Window.partitionBy("symbol", "source").orderBy(F.desc("time"))
        latest = (history.withColumn("_rank", F.row_number().over(newest))
                  .filter(F.col("_rank") == 1).drop("_rank"))
        latest.coalesce(1).write.mode("errorifexists").parquet(f"{OUTPUT}/latest")
        rows = history.count()
        latest_rows = spark.read.parquet(f"{OUTPUT}/latest").count()
        print(f"GOLD COMPLETED | Batch: {BATCH_ID}")
        print(f"History rows: {rows:,} | Latest rows: {latest_rows:,}")
        print(f"History: {OUTPUT}/history")
        print(f"Latest: {OUTPUT}/latest")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()