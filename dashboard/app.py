import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API = "http://127.0.0.1:8000"
st.set_page_config(page_title="VNStockPulse", layout="wide")

@st.cache_data(ttl=30)
def fetch(path, **params):
    response = requests.get(API + path, params=params, timeout=(5, 60))
    response.raise_for_status()
    return response.json()

def load(path, **params):
    try:
        return fetch(path, **params)
    except (requests.RequestException, ValueError) as exc:
        st.error(f"Không đọc được dữ liệu API: {exc}")
        st.stop()

def fmt(value, pattern=",.2f"):
    return "N/A" if pd.isna(value) else format(value, pattern)

st.title("VNStockPulse Dashboard")
health = load("/health")
st.caption(f"Batch: {health['latest_batch']} | Lịch sử: {health['history_rows']:,} dòng")
symbols = load("/symbols")["symbols"]
if not symbols:
    st.warning("Chưa có mã cổ phiếu.")
    st.stop()
symbol = st.sidebar.selectbox("Mã cổ phiếu", symbols,
                             index=symbols.index("FPT") if "FPT" in symbols else 0)
source = st.sidebar.text_input("Nguồn", "VCI").strip().upper()
limit = st.sidebar.slider("Số bản ghi", 50, 2000, 500, 50)
df = pd.DataFrame(load(f"/features/{symbol}", source=source, limit=limit)["data"])
if df.empty:
    st.warning("Không có dữ liệu cho mã và nguồn đã chọn.")
    st.stop()
df["time"] = pd.to_datetime(df["time"])
df = df.sort_values("time")
latest = df.iloc[-1]
st.caption(f"Ngày: {latest['time'].date()} | So sánh với: "
           f"{latest['previous_valid_date'] or 'N/A'} | "
           f"Khoảng cách: {fmt(latest['gap_calendar_days'], '.0f')} ngày lịch")
values = [("Giá đóng cửa", fmt(latest["close"])),
          ("So với bản ghi trước", fmt(latest["return_prev_valid"], ".2%")),
          ("MA20 bản ghi", fmt(latest["ma20"])), ("MA50 bản ghi", fmt(latest["ma50"]))]
for column, (label, value) in zip(st.columns(4), values):
    column.metric(label, value)
st.plotly_chart(px.line(df, x="time", y=["close", "ma20", "ma50"],
    title=f"{symbol} · Giá và trung bình động",
    color_discrete_sequence=["#2563eb", "#16a34a", "#dc2626"]), use_container_width=True)
st.plotly_chart(px.bar(df, x="time", y="volume", title=f"{symbol} · Khối lượng",
    color_discrete_sequence=["#0891b2"]), use_container_width=True)
st.subheader("Xếp hạng biến động so với bản ghi hợp lệ trước")
top = pd.DataFrame(load("/market/top-returns", period="return_prev_valid", limit=20)["data"])
if not top.empty:
    top["return_pct"] = top["return_value"] * 100
    st.dataframe(top[["symbol", "source", "time", "previous_valid_date",
                     "gap_calendar_days", "close", "return_pct"]],
                 use_container_width=True, hide_index=True)
else:
    st.info("Chưa có dữ liệu xếp hạng.")
with st.expander("Dữ liệu lịch sử"):
    st.dataframe(df.sort_values("time", ascending=False), use_container_width=True)