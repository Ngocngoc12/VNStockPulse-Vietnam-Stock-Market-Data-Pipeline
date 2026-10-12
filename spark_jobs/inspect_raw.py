from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VNStockPulse-Inspect-Raw")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json("/opt/raw/ohlcv_backfill/crawl_ohlcv_backfill_*.json")
    )
    print(f"Spark version: {spark.version}")
    print(f"Input files: {len(df.inputFiles())}")
    print("RAW JSON SCHEMA:")
    df.printSchema()
finally:
    spark.stop()