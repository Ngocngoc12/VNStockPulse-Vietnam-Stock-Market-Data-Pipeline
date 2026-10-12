import requests
import json
import re
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

# Nguồn: bài báo CafeBiz (không phải .gov.vn), trích số liệu gốc từ Cục Thống kê.
# LƯU Ý: đây là bài viết của tháng 8/2026 - phải tự tìm & cập nhật URL bài mới mỗi tháng
# (tìm trên cafebiz.vn/vi-mo.chn hoặc search "CPI tháng [X] [năm]")
URL = "https://cafebiz.vn/cpi-thang-8-2026-tang-047-176260903133717366.chn"


def crawl_cpi_news():
    print("Fetching CPI data from CafeBiz...")
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(URL, headers=headers, timeout=15)
    print("Status code:", response.status_code)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
    full_text = " ".join(paragraphs)

    def parse_percent(pattern, text=full_text):
        match = re.search(pattern, text)
        return float(match.group(1).replace(",", ".")) if match else None

    record = {
        "source": "CafeBiz (trích số liệu gốc từ Cục Thống kê)",
        "indicator": "CPI",
        "period": "2026-08",
        "month_over_month": parse_percent(r"tháng 8/2026 tăng ([0-9,]+)% so với tháng trước"),
        "year_over_year": parse_percent(r"tăng ([0-9,]+)% so với cùng kỳ năm trước"),
        "average_8_month_yoy": parse_percent(r"[Bb]ình quân tám tháng năm 2026, CPI tăng ([0-9,]+)%"),
        "core_inflation_avg_8_month": parse_percent(r"lạm phát cơ bản tăng ([0-9,]+)%"),
        "source_url": URL,
        "collected_at": datetime.now().isoformat()
    }

    if record["month_over_month"] is None:
        raise RuntimeError("Không tìm thấy đoạn dữ liệu CPI trong bài viết.")

    output_dir = Path("data/raw/gso")
    output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y_%m_%d")
    output_file = output_dir / f"crawl_cpi_news_{today}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump([record], f, ensure_ascii=False, indent=4)

    print(json.dumps(record, ensure_ascii=False, indent=4))
    print(f"Data saved to: {output_file}")
    return str(output_file)


if __name__ == "__main__":
    crawl_cpi_news()