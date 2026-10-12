from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BATCH_ID = "20261008_182058_4b127219"
BASE = "s3a://vnstockpulse"
SOURCE = f"{BASE}/bronze/ohlcv/batch_id={BATCH_ID}"
OUTPUT = f"{BASE}/silver/ohlcv/batch_id={BATCH_ID}"
INVALID_OUTPUT = f"{BASE}/dead_letter/ohlcv/batch_id={BATCH_ID}"


def main():
    spark = (
        SparkSession.builder.appName("VNStockPulse-Silver-MinIO")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.parquet(SOURCE)
        df = (raw.withColumn("raw_time", F.col("time"))
              .withColumn("symbol", F.upper(F.trim("symbol")))
              .withColumn("source", F.upper(F.trim("source")))
              .withColumn("time", F.to_date("time")))
        rules = [
            (F.length("symbol") > 0, "bad_symbol"),
            (F.length("source") > 0, "bad_source"),
            (F.col("time").isNotNull(), "bad_date"),
        ]
        for name in ["open", "high", "low", "close", "volume"]:
            c = F.col(name)
            finite = c.isNotNull() & ~F.isnan(c) & (F.abs(c) < float("inf"))
            positive = (c >= 0) if name == "volume" else (c > 0)
            rules.append((finite & positive, f"bad_{name}"))
        rules.append(((F.col("high") >= F.greatest("open", "close", "low")) &
                      (F.col("low") <= F.least("open", "close", "high")), "bad_range"))
        df = df.withColumn("invalid_reason", F.concat_ws(";",
            *[F.when(~F.coalesce(ok, F.lit(False)), reason) for ok, reason in rules]))
        invalid = df.filter(F.col("invalid_reason") != "")
        valid = df.filter(F.col("invalid_reason") == "").drop("invalid_reason")
        tie = F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(valid.columns)])), 256)
        window = Window.partitionBy("symbol", "time", "source").orderBy(
            F.to_timestamp("collected_at").desc_nulls_last(),
            F.col("ingested_at").desc_nulls_last(), tie.desc())
        silver = (valid.withColumn("_rank", F.row_number().over(window))
                  .filter(F.col("_rank") == 1).drop("_rank"))
        total, bad, before = raw.count(), invalid.count(), valid.count()
        invalid.write.mode("errorifexists").parquet(INVALID_OUTPUT)
        silver.repartition(8).write.mode("errorifexists").parquet(OUTPUT)
        after = spark.read.parquet(OUTPUT).count()
        print(f"SILVER COMPLETED | Batch: {BATCH_ID}")
        print(f"Original: {total:,} | Invalid: {bad:,}")
        print(f"Before dedup: {before:,} | Duplicates removed: {before-after:,}")
        print(f"Rows read back: {after:,}\nOutput: {OUTPUT}")
        print(f"Invalid records: {INVALID_OUTPUT}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()