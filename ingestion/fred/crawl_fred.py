import os
import json
from pathlib import Path
from datetime import datetime

from fredapi import Fred


# Danh sách FRED series cần thu thập
FRED_SERIES_LIST = {
    "DFF": "Fed Funds Effective Rate",
    "CPIAUCSL": "US CPI",
    "DTWEXBGS": "US Trade Weighted Dollar Index",
    "ID7108": "US Export Price Index - Gold",
}


def crawl_fred(series_id="DFF"):
    """
    Thu thập dữ liệu từ FRED API
    và lưu 30 giá trị gần nhất vào JSON.
    """

    # Lấy API key từ biến môi trường
    api_key = os.environ.get("FRED_API_KEY")

    if not api_key:
        raise RuntimeError(
            "FRED_API_KEY chưa được thiết lập."
        )

    print(f"Fetching FRED series: {series_id}...")

    # Kết nối FRED
    fred = Fred(api_key=api_key)

    # Lấy dữ liệu
    data = fred.get_series(series_id).tail(30)

    print(f"Extracted {len(data)} records.")

    if data.empty:
        raise ValueError(
            f"FRED {series_id} returned no data."
        )

    # Chuyển DataFrame/Series thành JSON records
    records = []

    for date, value in data.items():

        if value is None:
            continue

        records.append({
            "date": date.strftime("%Y-%m-%d"),
            "series_id": series_id,
            "series_name": FRED_SERIES_LIST.get(
                series_id,
                series_id
            ),
            "value": float(value)
        })

    # Tạo thư mục output
    output_dir = Path("data/raw/macro")
    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    # Tạo tên file theo ngày
    today = datetime.now().strftime("%Y_%m_%d")

    output_file = (
        output_dir
        / f"crawl_fred_{series_id}_{today}.json"
    )

    # Ghi JSON
    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            records,
            f,
            ensure_ascii=False,
            indent=4
        )

    print(
        f"Data saved to: {output_file}"
    )

    return str(output_file)


if __name__ == "__main__":

    print("Starting FRED ingestion...")

    for sid in FRED_SERIES_LIST:

        try:
            crawl_fred(
                series_id=sid
            )

        except Exception as e:

            print(
                f"  -> Bỏ qua {sid} do lỗi: {e}"
            )

    print("FRED ingestion completed.")