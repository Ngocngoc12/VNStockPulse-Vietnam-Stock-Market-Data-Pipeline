from pyspark.sql import SparkSession, functions as F

BATCH_ID = "20261008_182058_4b127219"
PATH = f"s3a://vnstockpulse/dead_letter/ohlcv/batch_id={BATCH_ID}"


def main():
    spark = (
        SparkSession.builder.appName("VNStockPulse-Inspect-Silver-Invalid")
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
        invalid = spark.read.parquet(PATH)

        print("\n=== COUNTS BY ERROR COMBINATION ===")
        (invalid.groupBy("invalid_reason").count()
         .orderBy(F.desc("count")).show(50, truncate=False))

        print("\n=== TOP SYMBOLS WITH FLAGGED ROWS ===")
        (invalid.groupBy("symbol").count()
         .orderBy(F.desc("count")).show(10, truncate=False))

        columns = [
            "symbol", "raw_time", "open", "high", "low",
            "close", "volume", "source", "invalid_reason",
        ]
        print("\n=== SAMPLE FLAGGED ROWS ===")
        invalid.select(*columns).show(10, truncate=False)

        print("\n=== SAMPLE OHLC RANGE FAILURES ===")
        (invalid.filter(F.col("invalid_reason").contains("bad_range"))
         .select(*columns).show(10, truncate=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()