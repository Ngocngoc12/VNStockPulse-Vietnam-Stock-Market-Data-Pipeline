import math
from pathlib import Path
from typing import Literal
import duckdb
from fastapi import FastAPI, HTTPException, Query

BATCH_ID = "20261008_182058_4b127219"
ROOT = Path(__file__).resolve().parents[1]
BATCH = ROOT / "data" / "serving" / "gold" / f"batch_id={BATCH_ID}"
app = FastAPI(title="VNStockPulse API", version="0.2.0")

def query(kind, sql, params=None):
    folder = BATCH / kind
    if not (BATCH / "manifest.json").is_file() or not any(folder.glob("*.parquet")):
        raise HTTPException(503, "Gold export is missing or incomplete")
    try:
        with duckdb.connect(":memory:") as con:
            con.read_parquet((folder / "*.parquet").as_posix()).create_view("gold")
            result = con.execute(sql, params or [])
            names = [column[0] for column in result.description]
            return [
                {name: None if isinstance(value, float) and not math.isfinite(value)
                 else value for name, value in zip(names, row)}
                for row in result.fetchall()
            ]
    except duckdb.Error as exc:
        raise HTTPException(503, "Cannot query Gold data; check server logs") from exc

@app.get("/")
@app.get("/health")
def health():
    history = query("history", "SELECT count(*) AS rows FROM gold")[0]["rows"]
    latest = query("latest", "SELECT count(*) AS rows FROM gold")[0]["rows"]
    return {"status": "ok", "latest_batch": BATCH_ID,
            "history_rows": history, "latest_rows": latest}

@app.get("/symbols")
def symbols():
    rows = query("latest", "SELECT DISTINCT symbol FROM gold ORDER BY symbol")
    return {"count": len(rows), "symbols": [row["symbol"] for row in rows]}

@app.get("/features/{symbol}")
def features(symbol: str, source: str = "VCI",
             limit: int = Query(default=300, ge=1, le=5000)):
    symbol, source = symbol.strip().upper(), source.strip().upper()
    rows = query("history", "SELECT * FROM gold WHERE symbol=? AND source=? "
                 "ORDER BY time DESC LIMIT ?", [symbol, source, limit])
    return {"symbol": symbol, "source": source, "count": len(rows), "data": rows}

@app.get("/market/latest")
def market_latest(limit: int = Query(default=100, ge=1, le=2000)):
    rows = query("latest", "SELECT * FROM gold ORDER BY symbol, source LIMIT ?", [limit])
    return {"count": len(rows), "data": rows}

@app.get("/market/top-returns")
def top_returns(period: Literal["return_prev_valid"] = "return_prev_valid",
                limit: int = Query(default=20, ge=1, le=100)):
    rows = query("latest", "SELECT *, return_prev_valid AS return_value FROM gold "
                 "WHERE time=(SELECT max(time) FROM gold) AND return_prev_valid IS NOT NULL "
                 "ORDER BY return_prev_valid DESC, symbol, source LIMIT ?", [limit])
    return {"period": period, "count": len(rows), "data": rows}
