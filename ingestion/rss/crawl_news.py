import feedparser
import json
from pathlib import Path
from datetime import datetime


RSS_SOURCES = {
    "CafeF": [
        "https://cafef.vn/thi-truong-chung-khoan.rss",
    ],

    "VietStock": [
        # Các feed Vietstock hiện có thể thay đổi theo chuyên mục.
        "https://vietstock.vn/5307/kinh-te.rss",
        "https://vietstock.vn/733/doanh-nghiep.rss",
    ],

    "CafeF-BDS": [
        "https://cafef.vn/bat-dong-san.rss",
    ],
}


def parse_feed(source_name, rss_urls):
    """
    Thử lần lượt các RSS URL của một nguồn.
    Trả về feed đầu tiên có dữ liệu.
    """

    for rss_url in rss_urls:
        print(f"  Trying: {rss_url}")

        try:
            feed = feedparser.parse(rss_url)

            status = getattr(feed, "status", None)
            entries_count = len(feed.entries)

            print(
                f"  -> status: {status}, "
                f"articles: {entries_count}"
            )

            if entries_count > 0:
                print(
                    f"  -> SUCCESS: sử dụng RSS {rss_url}"
                )
                return feed

        except Exception as e:
            print(
                f"  -> RSS lỗi: {rss_url} | {e}"
            )

    print(
        f"  -> Không tìm thấy RSS hoạt động cho {source_name}"
    )

    return None


def crawl_news():
    all_records = []

    for source_name, rss_urls in RSS_SOURCES.items():

        print(f"\nFetching news from {source_name}...")

        feed = parse_feed(
            source_name,
            rss_urls
        )

        if feed is None:
            continue

        for entry in feed.entries:

            all_records.append({
                "source": source_name,
                "title": entry.get("title"),
                "link": entry.get("link"),
                "summary": entry.get("summary"),
                "published": entry.get("published"),
                "id": entry.get("id"),
                "collected_at": datetime.now().isoformat()
            })

    if not all_records:
        raise ValueError(
            "Không có bài viết nào từ bất kỳ nguồn RSS nào."
        )

    output_dir = Path("data/raw/news")
    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    today = datetime.now().strftime("%Y_%m_%d")

    output_file = (
        output_dir
        / f"crawl_news_{today}.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            all_records,
            f,
            ensure_ascii=False,
            indent=4
        )

    print(
        f"\nTổng {len(all_records)} bài viết, "
        f"lưu tại: {output_file}"
    )

    return str(output_file)


if __name__ == "__main__":
    crawl_news()