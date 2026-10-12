"""
Crawl OHLCV HÀNG NGÀY — chạy sau giờ đóng cửa mỗi ngày qua Airflow.

Khác với crawl_ohlcv_backfill.py (chạy 1 lần, lấy 10 năm lịch sử), file
này chỉ lấy 1-2 ngày gần nhất, chạy cho TOÀN BỘ mã mỗi lần (không dùng
checkpoint resume theo nghĩa "còn thiếu" — vì đây là dữ liệu mới của
ngày mới, không phải phần backfill còn sót lại).
"""
import json
import time
import os
from pathlib import Path
from datetime import datetime, timedelta

from vnstock import Vnstock
from tqdm import tqdm


# ============================================================
# CẤU HÌNH API KEY (tăng rate limit từ 20 -> 60 request/phút)
# ============================================================
from dotenv import load_dotenv

load_dotenv()

VNSTOCK_API_KEY = os.environ.get("VNSTOCK_API_KEY", "").strip()

if VNSTOCK_API_KEY:
    print("VNStock API key detected.")
else:
    print("VNSTOCK_API_KEY is not set; using community mode.")


# ============================================================
# CONFIGURATION
# ============================================================

OHLCV_SOURCE = "VCI"

# Chỉ lấy 2 ngày gần nhất (hôm nay + hôm qua làm buffer, tránh thiếu dữ
# liệu nếu API trả chậm 1 nhịp so với giờ đóng cửa thật)
OHLCV_DAYS = 2

# Delay giữa các request (giây)
REQUEST_DELAY = 5.0

# Số lần retry nếu API lỗi
MAX_RETRIES = 3

# Đánh dấu đã chạy xong hôm nay chưa - để tránh chạy trùng lặp nếu lịch
# Task Scheduler/Airflow vô tình kích hoạt 2 lần trong cùng 1 ngày
DAILY_MARKER_DIR = Path("data/checkpoint")


# ============================================================
# DAILY RUN MARKER (khác checkpoint resume của backfill)
# ============================================================

def _daily_marker_file():
    today = datetime.now().strftime("%Y_%m_%d")
    return DAILY_MARKER_DIR / f"daily_ohlcv_done_{today}.marker"


def already_ran_today():
    """True nếu daily crawl đã chạy xong hôm nay rồi."""
    return _daily_marker_file().exists()


def mark_daily_done():
    """Đánh dấu daily crawl hôm nay đã hoàn tất toàn bộ."""
    DAILY_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    _daily_marker_file().touch()


# ============================================================
# LOAD SYMBOLS
# ============================================================

def load_symbols_from_companies():
    """Đọc danh sách symbol từ file companies mới nhất."""
    companies_dir = Path("data/raw/companies")

    files = sorted(
        companies_dir.glob("crawl_companies_*.json"),
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )

    if not files:
        raise FileNotFoundError(
            "Không tìm thấy file companies trong data/raw/companies/"
        )

    latest_file = files[0]
    print(f"Loading companies from: {latest_file}")

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    symbols = []
    for item in data:
        symbol = item.get("symbol")
        if symbol:
            symbol = str(symbol).strip().upper()
            if symbol:
                symbols.append(symbol)

    symbols = sorted(set(symbols))
    print(f"Loaded {len(symbols)} unique symbols.")
    return symbols


# ============================================================
# CRAWL ONE SYMBOL (chỉ lấy vài ngày gần nhất)
# ============================================================

def crawl_ohlcv_daily(symbol, source=OHLCV_SOURCE, days=OHLCV_DAYS):
    """Crawl OHLCV mới nhất (vài ngày gần nhất) cho một mã cổ phiếu."""

    print(f"Fetching daily OHLCV: {symbol}")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    print(
        f"Period: {start_date.strftime('%Y-%m-%d')} -> "
        f"{end_date.strftime('%Y-%m-%d')}"
    )

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            stock = Vnstock().stock(symbol=symbol, source=source)
            data = stock.quote.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
            )
            break

        except SystemExit as exc:
            # Ensure a library exit cannot mark this crawl successful.
            print(
                f"{symbol}: VNStock stopped the process. "
                "Check quota/authentication before retrying.",
                flush=True,
            )
            raise SystemExit(1) from exc

        except Exception as exc:
            last_error = exc
            message = str(exc).lower()

            if any(term in message for term in (
                "rate limit", "ratelimit", "429",
                "hạn mức", "han muc", "too many requests",
            )):
                print(
                    f"{symbol}: quota reached; stopping this run. {exc}",
                    flush=True,
                )
                raise SystemExit(1) from exc

            print(f"Attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                wait_time = 30 * attempt
                print(f"Retry after {wait_time}s...", flush=True)
                time.sleep(wait_time)
    else:
        raise RuntimeError(
            f"{symbol}: failed after {MAX_RETRIES} attempts. "
            f"Last error: {last_error}"
        )

    if data is None:
        raise ValueError(f"{symbol}: API trả về None.")

    if data.empty:
        raise ValueError(f"{symbol}: Không có dữ liệu OHLCV.")

    print(f"Extracted {len(data)} OHLCV records.")

    # Convert DataFrame -> JSON records
    records = []
    for _, row in data.iterrows():
        record = {}
        for column in data.columns:
            value = row[column]
            try:
                if value != value:  # NaN check
                    value = None
            except Exception:
                pass
            if hasattr(value, "item"):
                try:
                    value = value.item()
                except Exception:
                    pass
            if hasattr(value, "strftime"):
                try:
                    value = value.strftime("%Y-%m-%d")
                except Exception:
                    pass
            record[column] = value

        record["symbol"] = symbol
        record["source"] = source
        record["collected_at"] = datetime.now().isoformat()
        records.append(record)

    # Lưu vào thư mục riêng cho daily, để không lẫn với dữ liệu backfill
    output_dir = Path("data/raw/ohlcv_daily")
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y_%m_%d")
    output_file = output_dir / f"crawl_ohlcv_daily_{symbol}_{today}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=4, default=str)

    print(f"Data saved to: {output_file}")
    return str(output_file)


# ============================================================
# CRAWL ALL SYMBOLS - DAILY
# ============================================================

def crawl_all_stocks_daily(limit=None) -> list[dict]:
    is_test = limit is not None
    if is_test and (not isinstance(limit, int) or limit < 1):
        raise ValueError("limit must be a positive integer.")

    if not is_test and already_ran_today():
        print("Daily checkpoint exists; skipping crawl.")
        return []

    symbols = list(load_symbols_from_companies())
    if not symbols:
        raise RuntimeError("No symbols loaded.")
    if is_test:
        symbols = symbols[:limit]

    print(f"Mode: {'TEST' if is_test else 'FULL'} | Symbols: {len(symbols)}")
    results = []
    for symbol in tqdm(symbols, desc="Daily OHLCV update"):
        try:
            output_file = crawl_ohlcv_daily(
                symbol=symbol, source=OHLCV_SOURCE, days=OHLCV_DAYS
            )
            results.append({
                "symbol": symbol, "status": "SUCCESS",
                "output_file": str(output_file),
            })
        except Exception as exc:
            print(f"{symbol}: FAILED: {exc}", flush=True)
            results.append({
                "symbol": symbol, "status": "FAILED", "error": str(exc),
            })
        time.sleep(REQUEST_DELAY)

    success = sum(r["status"] == "SUCCESS" for r in results)
    failed = len(results) - success
    report_dir = Path("data/raw/ohlcv_daily")
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    mode = "test" if is_test else "full"
    report_file = report_dir / f"ohlcv_daily_report_{mode}_{stamp}.json"
    with report_file.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=4)

    print(f"Success: {success} | Failed: {failed} | Report: {report_file}")
    if failed:
        raise RuntimeError(f"{failed}/{len(symbols)} symbols failed; no checkpoint.")
    if not is_test:
        mark_daily_done()
    else:
        print("TEST completed; daily checkpoint was not updated.")
    return results


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Starting DAILY OHLCV update...")
    crawl_all_stocks_daily()
    print("\nDaily OHLCV update completed.")
