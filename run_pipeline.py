from lakehouse.bronze_ohlcv import main as run_bronze
from lakehouse.silver_ohlcv import main as run_silver
from lakehouse.gold_ohlcv_features import main as run_gold


def main() -> None:
    print("=" * 70)
    print("VNStockPulse - Full OHLCV Pipeline")
    print("=" * 70)

    print("\n[1/3] Running Bronze...")
    run_bronze()

    print("\n[2/3] Running Silver...")
    run_silver()

    print("\n[3/3] Running Gold...")
    run_gold()

    print("\n" + "=" * 70)
    print("Pipeline completed.")
    print("=" * 70)


if __name__ == "__main__":
    main()