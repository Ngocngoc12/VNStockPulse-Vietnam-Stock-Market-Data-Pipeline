# Data Source Verification

## 1. VNStock - OHLCV

- Status: PASS
- Symbol tested: FPT
- Source: VCI
- Records: 24
- Fields:
  - time
  - open
  - high
  - low
  - close
  - volume
- Output:
  `data/raw/ohlcv/crawl_ohlcv_FPT_2026_09_06.json`

## 2. VNStock - Company Listing

- Status: PASS
- Records: 1522
- Fields:
  - symbol
  - organ_name
- Output:
  `data/raw/companies/crawl_companies_2026_09_06.json`

## 3. CafeF RSS

- Status: PASS
- HTTP status: 200
- Articles: 50
- Fields:
  - title
  - link
  - summary
  - published
  - published_parsed
  - id
  - guidislink

## 4. FRED

- Status: PASS
- Series tested: DFF
- Records: 26363
- Output:
  `data/raw/macro/crawl_fred_DFF_2026_09_06.json`

## 5. SBV

- Status: FAIL
- HTTP status: 200
- Actual response: Request Rejected
- Data extracted: No
- Reason:
  SBV server/WAF rejected the request.
  ## OHLCV Backfill
- 1521/1522 mã thành công (99.93%)
- Mã lỗi: UTT — API trả về dữ liệu rỗng (khả năng đã hủy đăng ký giao dịch 
  hoặc chưa từng có giao dịch thật, dù vẫn xuất hiện trong danh sách công ty)