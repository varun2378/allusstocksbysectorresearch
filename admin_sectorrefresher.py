# File: admin_sectorrefresh.py
import os
import glob
import pandas as pd
import requests
import json
import hashlib
import streamlit as st
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
API_KEY = "QLSJQLW02XODEZUS"
data_folder = "./StockAnalysisData"
cache_dir = os.path.join(data_folder, "api_cache")
os.makedirs(cache_dir, exist_ok=True)

# --- Utility Functions ---
def get_cache_filename(function, symbol):
    key = f"{function}_{symbol}"
    filename = hashlib.md5(key.encode()).hexdigest() + ".json"
    return os.path.join(cache_dir, filename)

def fetch_and_cache(function, symbol):
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": API_KEY}
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if 'Information' in data and 'limit' in data['Information'].lower():
                st.warning(f"⚠️ Rate limit hit while fetching {function} for {symbol}")
                return None
            filename = get_cache_filename(function, symbol)
            with open(filename, 'w') as f:
                json.dump(data, f)
            return data
        except Exception as e:
            st.error(f"❌ Error fetching {function} for {symbol}: {e}")
            time.sleep(1)
    return None

def refresh_symbols(symbols, sector_name=None, update_queue=None):
    status = []
    for i, symbol in enumerate(symbols):
        fetch_and_cache("OVERVIEW", symbol)
        fetch_and_cache("INCOME_STATEMENT", symbol)
        fetch_and_cache("BALANCE_SHEET", symbol)
        fetch_and_cache("TIME_SERIES_DAILY_ADJUSTED", symbol)
        if update_queue:
            update_queue.append(f"📦 {sector_name}: {i+1}/{len(symbols)} - {symbol}")
        status.append(symbol)
    return f"✅ Completed {len(status)} stocks in {sector_name or 'manual'}"

def refresh_sector_concurrently(sector, symbols, update_queue):
    return refresh_symbols(symbols, sector_name=sector, update_queue=update_queue)

# --- Streamlit UI ---
st.set_page_config(page_title="Admin Sector Refresher", layout="wide")
st.title("🛠️ Admin Tool: Refresh Stock Data by Sector")

# Load sectors from Excel files
excel_files = glob.glob(os.path.join(data_folder, "sp500_*_stocks.xlsx"))
sector_map = {}
for file in excel_files:
    sector = os.path.basename(file).replace("sp500_", "").replace("_stocks.xlsx", "").replace("_", " ")
    try:
        df = pd.read_excel(file)
        symbols = df["Symbol"].dropna().tolist()
        if symbols:
            sector_map[sector] = symbols
    except Exception as e:
        st.warning(f"⚠️ Error loading {file}: {e}")

# --- UI Option 1: Refresh a selected sector ---
selected_sector = st.selectbox("📂 Select Sector to Refresh", options=list(sector_map.keys()))
symbols = sector_map.get(selected_sector, [])

if st.button("🔄 Refresh Selected Sector"):
    if not symbols:
        st.warning("No symbols found for this sector.")
    else:
        with st.spinner(f"Refreshing {len(symbols)} stocks in '{selected_sector}'..."):
            refresh_symbols(symbols, selected_sector)
        st.success(f"✅ Sector '{selected_sector}' refreshed.")

# --- UI Option 2: Refresh all sectors concurrently ---
if st.button("🚀 Refresh All Sectors Concurrently (2 at a time)"):
    st.info("Running all sector refreshes concurrently...")
    overall_progress = st.progress(0)
    completed = []
    total_sectors = len(sector_map)
    sector_updates = st.container()

    update_queues = {sector: [] for sector in sector_map}

    def process_updates():
        for sector, queue in update_queues.items():
            if queue:
                st.info(queue.pop(0))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(refresh_sector_concurrently, sector, symbols, update_queues[sector]): sector
            for sector, symbols in sector_map.items()
        }
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            completed.append(result)
            overall_progress.progress((i + 1) / total_sectors)
            with sector_updates:
                st.success(result)

    st.success("✅ All sector refreshes completed.")
