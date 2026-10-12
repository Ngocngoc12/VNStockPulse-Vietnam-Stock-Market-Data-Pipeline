import feedparser


RSS_URL = "https://cafef.vn/thi-truong-chung-khoan.rss"


print("Testing RSS...")
print(f"URL: {RSS_URL}")

feed = feedparser.parse(RSS_URL)


print("\nStatus:", feed.status)
print("Number of articles:", len(feed.entries))


if feed.entries:
    print("\nFirst article:")
    print("Title:", feed.entries[0].get("title"))
    print("Link:", feed.entries[0].get("link"))
    print("Published:", feed.entries[0].get("published"))
    print("Available fields:")
    print(feed.entries[0].keys())
else:
    print("\nNo articles found.")