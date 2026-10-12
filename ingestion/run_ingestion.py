from ingestion.vnstock.crawl_companies import crawl_companies
from ingestion.vnstock.crawl_ohlcv_daily import crawl_all_stocks_daily
from ingestion.rss.crawl_news import crawl_news
from ingestion.fred.crawl_fred import crawl_fred, FRED_SERIES_LIST
from ingestion.gso.crawl_cpi import crawl_cpi_news
from ingestion.sbv.crawl_sbv import crawl_sbv


def main():
    print("Starting VNStockPulse ingestion...")

    # ==================================================
    # [1] COMPANIES
    # ==================================================
    print("\n[1] Companies")

    try:
        crawl_companies()
    except Exception as e:
        print(f"  -> Companies lỗi: {e}")


    # ==================================================
    # [2] OHLCV (daily update - toàn bộ mã, 1-2 ngày gần nhất)
    # ==================================================
    print("\n[2] OHLCV (daily)")

    try:
        crawl_all_stocks_daily()
    except Exception as e:
        print(f"  -> OHLCV daily lỗi: {e}")


    # ==================================================
    # [3] NEWS
    # ==================================================
    print("\n[3] News")

    try:
        crawl_news()
    except Exception as e:
        print(f"  -> News lỗi: {e}")


    # ==================================================
    # [4] FRED
    # ==================================================
    print("\n[4] FRED")

    for sid in FRED_SERIES_LIST:
        try:
            print(f"  -> Crawling FRED {sid}...")
            crawl_fred(
                series_id=sid
            )

        except Exception as e:
            print(
                f"  -> Bỏ qua {sid} do lỗi: {e}"
            )


    # ==================================================
    # [5] GSO (CPI)
    # ==================================================
    print("\n[5] GSO")

    try:
        crawl_cpi_news()
    except Exception as e:
        print(f"  -> GSO lỗi: {e}")


    # ==================================================
    # [6] SBV
    # ==================================================
    print("\n[6] SBV")

    try:
        crawl_sbv()
    except Exception as e:
        print(f"  -> SBV lỗi: {e}")


    # ==================================================
    # DONE
    # ==================================================
    print("\nVNStockPulse ingestion completed.")


if __name__ == "__main__":
    main()