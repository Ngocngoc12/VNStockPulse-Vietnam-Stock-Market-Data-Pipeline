import json
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup


URL = "https://www.fexant.com/currency-pair/SBV/USD-VND"


def crawl_sbv():
    print("Fetching SBV USD/VND exchange rate...")

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(
        URL,
        headers=headers,
        timeout=15
    )

    print(f"Status code: {response.status_code}")
    response.raise_for_status()

    # Lưu raw HTML
    output_dir = Path("data/raw/sbv")
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y_%m_%d")

    raw_file = output_dir / f"raw_sbv_usdvnd_{today}.html"

    raw_file.write_text(
        response.text,
        encoding="utf-8"
    )

    print(f"Raw data saved to: {raw_file}")

    soup = BeautifulSoup(response.text, "html.parser")

    # Tìm bảng lịch sử tỷ giá
    rows = soup.find_all("tr")

    records = []

    for row in rows:
        cells = row.find_all("td")

        if len(cells) < 2:
            continue

        values = [
            cell.get_text(" ", strip=True)
            for cell in cells
        ]

        # Kiểm tra dòng có ngày YYYY-MM-DD
        date_text = values[0]

        try:
            date_obj = datetime.strptime(
                date_text,
                "%Y-%m-%d"
            )
        except ValueError:
            continue

        # Tìm giá trị VND trong các cell
        rate = None

        for value in values[1:]:
            if "VND" not in value:
                continue

            cleaned = (
                value
                .replace("VND", "")
                .replace(" ", "")
                .replace(",", "")
            )

            try:
                rate = float(cleaned)
                break
            except ValueError:
                continue

        if rate is not None:
            records.append({
                "date": date_obj.strftime("%Y-%m-%d"),
                "rate": rate
            })

    print(f"Extracted {len(records)} SBV records.")

    if not records:
        raise ValueError(
            "Không tìm thấy dữ liệu tỷ giá trong bảng SBV."
        )

    # Sắp xếp từ ngày mới nhất -> cũ nhất
    records.sort(
        key=lambda x: x["date"],
        reverse=True
    )

    # Lấy record mới nhất
    latest = records[0]

    record = {
        "source": "SBV",
        "indicator": "USD_VND",
        "currency": "USD",
        "rate": latest["rate"],
        "date": latest["date"],
        "collected_date": datetime.now().strftime("%Y-%m-%d"),
        "source_url": URL
    }

    output_file = (
        output_dir /
        f"crawl_sbv_usdvnd_{today}.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            record,
            f,
            ensure_ascii=False,
            indent=4
        )

    print("\nExtracted SBV record:")

    print(
        json.dumps(
            record,
            ensure_ascii=False,
            indent=4
        )
    )

    print(
        f"\nData saved to: {output_file}"
    )


if __name__ == "__main__":
    crawl_sbv()