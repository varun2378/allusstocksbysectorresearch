import os
import json
import requests
import hashlib
import streamlit as st
from datetime import datetime

# --- Config ---
API_KEY = "QLSJQLW02XODEZUS"  # Your premium Alpha Vantage API Key
cache_dir = "./StockAnalysisData/api_cache"
os.makedirs(cache_dir, exist_ok=True)

# --- Helper to get cache filename ---
def get_cache_filename(function, symbol):
    key = f"{function}_{symbol}"
    filename = hashlib.md5(key.encode()).hexdigest() + ".json"
    return os.path.join(cache_dir, filename)

# --- Fetch data from Alpha Vantage or cache ---
def fetch_and_save(function, symbol):
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": API_KEY}
    response = requests.get(url, params=params)
    try:
        data = response.json()
    except Exception:
        st.error(f"❌ Failed to parse response for {function}")
        return None
# rate limit check
    # Handle rate limit
    if "Information" in data and "limit" in data["Information"].lower():
        st.warning(f"⚠️ Rate limit hit for {function}")
        return None

    # Save to JSON
    filename = get_cache_filename(function, symbol)
    with open(filename, 'w') as f:
        json.dump(data, f)
    return data

# --- UI ---
st.title("🔧 Admin: Refresh Stock Symbol")

symbol = st.text_input("Enter stock symbol to refresh (e.g., AAPL, MSFT):").upper()
if st.button("🔄 Refresh Data"):
    if not symbol:
        st.warning("Please enter a valid symbol.")
    else:
        st.info(f"Refreshing data for {symbol}...")
        overview = fetch_and_save("OVERVIEW", symbol)
        income = fetch_and_save("INCOME_STATEMENT", symbol)
        balance = fetch_and_save("BALANCE_SHEET", symbol)
        prices = fetch_and_save("TIME_SERIES_DAILY_ADJUSTED", symbol)

        if overview:
            st.success(f"✅ {symbol} overview refreshed and saved.")
            st.subheader("📋 Company Overview")
            st.json(overview)

        if income:
            st.success("✅ Income statement updated.")
        if balance:
            st.success("✅ Balance sheet updated.")
        if prices:
            st.success("✅ Recent prices updated.")

st.markdown("---")
st.caption("Data is stored as JSON in `./StockAnalysisData/api_cache/` for integration with your main dashboard.")
