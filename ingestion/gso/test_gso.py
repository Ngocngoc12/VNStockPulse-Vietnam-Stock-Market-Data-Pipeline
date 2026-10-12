import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


URL = "https://www.nso.gov.vn/cpi-vi/"

headers = {
    "User-Agent": "Mozilla/5.0"
}

print("Finding GSO CPI article...")

response = requests.get(
    URL,
    headers=headers,
    timeout=15
)

print("Status code:", response.status_code)

soup = BeautifulSoup(response.text, "html.parser")


target = "Chỉ số giá tiêu dùng (CPI), chỉ số giá vàng và chỉ số giá đô la Mỹ tháng Tám và 8 tháng năm 2026"


for a in soup.find_all("a", href=True):

    title = a.get_text(" ", strip=True)

    if "tháng Tám và 8 tháng năm 2026" in title:

        article_url = urljoin(URL, a["href"])

        print("\nFOUND ARTICLE:")
        print("Title:", title)
        print("URL:", article_url)

        break
else:
    print("\nKhông tìm thấy link bài CPI.")