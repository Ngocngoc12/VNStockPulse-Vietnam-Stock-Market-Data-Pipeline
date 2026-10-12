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

VNSTOCK_API_KEY = os.environ.get("VNSTOCK_API_KEY")
if not VNSTOCK_API_KEY:
    raise RuntimeError("Missing VNSTOCK_API_KEY in .env")


# ============================================================
# CONFIGURATION
# ============================================================

# Chế độ chạy:
# "WATCHLIST" -> chỉ chạy 8 mã test
# "ALL"       -> chạy toàn bộ mã lấy được từ companies
OHLCV_MODE = "ALL"

# Các mã dùng để TEST nhanh trước khi chạy ALL
WATCHLIST = [
    "FPT",
    "VNM",
    "HPG",
    "VCB",
    "MWG",
    "SSI",
    "VIC",
    "MSN",
]

# Nguồn dữ liệu
OHLCV_SOURCE = "VCI"

# Số ngày lịch sử cần lấy - 10 năm cho backfill
OHLCV_DAYS = 365 * 10

# Delay giữa các request (giây)
REQUEST_DELAY = 1.0

# Số lần retry nếu API lỗi
MAX_RETRIES = 3

# Thư mục lưu checkpoint tiến độ (để resume nếu bị dừng giữa chừng -
# QUAN TRỌNG với backfill vì 10 năm x 1500+ mã sẽ chạy rất lâu)
CHECKPOINT_DIR = Path("data/checkpoint")


# ============================================================
# CHECKPOINT (RESUME) HELPERS
# ============================================================

def _checkpoint_file():
    """
    Checkpoint riêng cho backfill, KHÔNG theo ngày như daily - vì backfill
    là tác vụ chạy 1 lần, có thể kéo dài nhiều ngày liên tục nếu bị ngắt
    quãng nhiều lần, nên không nên reset theo ngày.
    """
    return CHECKPOINT_DIR / "ohlcv_backfill_progress.json"


def load_completed_symbols():
    """Đọc danh sách mã đã backfill xong (nếu có), để bỏ qua khi resume."""
    f = _checkpoint_file()
    if not f.exists():
        return set()
    with open(f, "r", encoding="utf-8") as fh:
        return set(json.load(fh))


def mark_symbol_done(symbol):
    """Ghi nhận 1 mã đã backfill xong vào checkpoint."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    completed = load_completed_symbols()
    completed.add(symbol)
    with open(_checkpoint_file(), "w", encoding="utf-8") as fh:
        json.dump(sorted(completed), fh, ensure_ascii=False, indent=2)


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
# CRAWL ONE SYMBOL
# ============================================================

def crawl_ohlcv(symbol, source=OHLCV_SOURCE, days=OHLCV_DAYS):
    """Crawl historical OHLCV (10 năm) cho một mã cổ phiếu."""

    print(f"Fetching OHLCV: {symbol}")

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
                end=end_date.strftime("%Y-%m-%d")
            )
            break
        except Exception as e:
            last_error = e
            print(f"  -> Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                wait_time = attempt * 2
                print(f"  -> Retry after {wait_time}s...")
                time.sleep(wait_time)
    else:
        raise RuntimeError(
            f"Không thể crawl {symbol} sau {MAX_RETRIES} lần thử. "
            f"Lỗi cuối: {last_error}"
        )

    if data is None:
        raise ValueError(f"{symbol}: API trả về None.")

    if data.empty:
        raise ValueError(f"{symbol}: Không có dữ liệu OHLCV.")

    print(f"Extracted {len(data)} OHLCV records.")
    print(f"Columns: {list(data.columns)}")
    print("\nFirst 5 records:")
    print(data.head())

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

    # Lưu vào thư mục riêng cho backfill, tách biệt với dữ liệu daily
    output_dir = Path("data/raw/ohlcv_backfill")
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y_%m_%d")
    output_file = output_dir / f"crawl_ohlcv_backfill_{symbol}_{today}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=4, default=str)

    print(f"Data saved to: {output_file}")
    return str(output_file)


# ============================================================
# CRAWL ALL SYMBOLS
# ============================================================

def crawl_all_stocks(max_symbols=None):
    """
    Backfill OHLCV 10 năm cho toàn bộ symbols.

    max_symbols:
        None -> toàn bộ
        20   -> test 20 mã
        50   -> test 50 mã

    Có checkpoint: nếu chạy lại (bất kể ngày nào), các mã đã backfill
    thành công trước đó sẽ được bỏ qua (resume), không cào lại từ đầu.
    """

    symbols = load_symbols_from_companies()

    if max_symbols is not None:
        symbols = symbols[:max_symbols]
        print(f"TEST MODE: chỉ crawl {len(symbols)} symbols.")

    already_done = load_completed_symbols()
    remaining = [s for s in symbols if s not in already_done]

    print("\n")
    print("=" * 70)
    print("VNStockPulse - BACKFILL 10 YEARS OHLCV")
    print("=" * 70)
    print(f"Tổng số mã: {len(symbols)}")
    print(f"Đã xong (checkpoint): {len(already_done)}")
    print(f"Còn lại cần crawl: {len(remaining)}")

    results = []
    total = len(remaining)

    for symbol in tqdm(remaining, desc="Backfill OHLCV 10y"):
        try:
            output_file = crawl_ohlcv(symbol=symbol, source=OHLCV_SOURCE, days=OHLCV_DAYS)
            results.append({"symbol": symbol, "status": "SUCCESS", "output_file": output_file})
            mark_symbol_done(symbol)
        except Exception as e:
            print(f"  -> Bỏ qua {symbol} do lỗi: {e}")
            results.append({"symbol": symbol, "status": "FAILED", "error": str(e)})

        time.sleep(REQUEST_DELAY)

    success = sum(1 for r in results if r["status"] == "SUCCESS")
    failed = sum(1 for r in results if r["status"] == "FAILED")

    print("\n")
    print("=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"Đã bỏ qua (checkpoint từ trước): {len(already_done)}")
    print(f"Chạy lần này  : {total}")
    print(f"Success       : {success}")
    print(f"Failed        : {failed}")

    report_dir = Path("data/raw/ohlcv_backfill")
    today = datetime.now().strftime("%Y_%m_%d")
    report_file = report_dir / f"ohlcv_backfill_report_{today}.json"

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print(f"\nBackfill report saved to: {report_file}")

    return results


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print("Starting OHLCV BACKFILL (10 years)...")

    if OHLCV_MODE == "WATCHLIST":

        print("\nMode: WATCHLIST")

        for symbol in WATCHLIST:
            try:
                crawl_ohlcv(symbol=symbol, source=OHLCV_SOURCE, days=OHLCV_DAYS)
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                print(f"  -> Bỏ qua {symbol} do lỗi: {e}")

    elif OHLCV_MODE == "ALL":

        print("\nMode: ALL STOCKS")
        crawl_all_stocks()

    else:
        raise ValueError(
            f"OHLCV_MODE không hợp lệ: {OHLCV_MODE}. Chỉ dùng WATCHLIST hoặc ALL."
        )

    print("\nOHLCV backfill completed.")