from vnstock.api.listing import Listing
from pathlib import Path
import json
from datetime import datetime


def crawl_companies():

    print("Fetching company listing...")

    # =========================
    # EXTRACT
    # =========================

    listing = Listing()
    df = listing.all_symbols()

    # =========================
    # CHECK DATA
    # =========================

    print(f"Extracted {len(df)} company records.")
    print("\nColumns:")
    print(df.columns.tolist())
    print("\nFirst 5 records:")
    print(df.head())

    if df.empty:
        raise ValueError("Company listing returned no data.")

    # =========================
    # CONVERT TO JSON
    # =========================

    records = df.to_dict(orient="records")

    # =========================
    # SAVE RAW DATA
    # =========================

    output_dir = Path("data/raw/companies")
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y_%m_%d")
    output_file = output_dir / f"crawl_companies_{today}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=4, default=str)

    print(f"\nData saved to: {output_file}")
    return str(output_file)


if __name__ == "__main__":
    crawl_companies()