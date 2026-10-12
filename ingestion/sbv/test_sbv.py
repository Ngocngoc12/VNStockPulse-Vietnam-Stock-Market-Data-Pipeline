import requests


URL = "https://www.sbv.gov.vn/webcenter/portal/vi/menu/rm/ktt/tygia/tygiatrungtam"

headers = {
    "User-Agent": "Mozilla/5.0"
}


print("Testing SBV...")
print("URL:", URL)


try:
    response = requests.get(
        URL,
        headers=headers,
        timeout=10
    )

    print("Status code:", response.status_code)

    print("\nResponse preview:")
    print(response.text[:500])

except requests.RequestException as e:
    print("Request error:", e)