import os
import glob
import pandas as pd
import requests
import time
import streamlit as st
import io
import json
import hashlib
import streamlit as st
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Sector-Wise Stock Financials", layout="wide")

API_KEY = "QLSJQLW02XODEZUS"
data_folder = r"./StockAnalysisData"
cache_dir = r"./StockAnalysisData/api_cache"
os.makedirs(cache_dir, exist_ok=True)

last_updated_map = {}

sector_symbols_map = {}
excel_files = glob.glob(os.path.join(data_folder, "sp500_*_stocks.xlsx"))
for file in excel_files:
    try:
        df = pd.read_excel(file)
        sector = os.path.basename(file).replace("sp500_", "").replace("_stocks.xlsx", "").replace("_", " ")
        symbols = df['Symbol'].dropna().tolist()
        if symbols:
            sector_symbols_map[sector] = symbols
            print(f"✅ Loaded symbols from {sector}: {symbols}")
    except Exception as e:
        print(f"⚠️ Skipping {file}: {e}")

all_symbols = [symbol for symbol_list in sector_symbols_map.values() for symbol in symbol_list]

@st.cache_data(ttl=86400)
def fetch_data(function, symbol):
    key_string = f"{function}_{symbol}"
    filename = os.path.join(cache_dir, hashlib.md5(key_string.encode()).hexdigest() + ".json")
    if os.path.exists(filename):
        file_mtime = os.path.getmtime(filename)
        file_age = time.time() - file_mtime
        last_updated_map[symbol] = datetime.fromtimestamp(file_mtime).strftime("%Y-%m-%d %H:%M:%S")
        if file_age < 86400:
            try:
                with open(filename, 'r') as f:
                    return json.load(f)
            except:
                pass

    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": API_KEY}
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if 'Information' in data and 'limit' in data['Information'].lower():
                st.warning(f"⚠️ API rate limit hit for {symbol}. Retrying ({attempt+1})...")
                time.sleep(1)
                continue
            with open(filename, 'w') as f:
                json.dump(data, f)
            last_updated_map[symbol] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return data
        except:
            time.sleep(1)
    return {}

def get_last_7_prices(symbol):
    data = fetch_data("TIME_SERIES_DAILY_ADJUSTED", symbol)
    ts = data.get("Time Series (Daily)", {})
    prices = []
    for date in sorted(ts.keys(), reverse=True)[:7]:
        price = ts[date].get("5. adjusted close", None)
        if price:
            prices.append(float(price))
    return prices

def extract_quarterly(data, keys, prefix):
    result = {}
    if "quarterlyReports" in data:
        for i, report in enumerate(data["quarterlyReports"][:4]):
            for key in keys:
                col = f"{prefix}_{key}_Q{i+1}"
                result[col] = report.get(key, None)
    return result

def process_symbol(symbol):
    for attempt in range(2):  # Try twice
        try:
            overview = fetch_data("OVERVIEW", symbol)
            income_q = fetch_data("INCOME_STATEMENT", symbol)
            balance_q = fetch_data("BALANCE_SHEET", symbol)

            if not overview:
                st.warning(f"❌ Skipping {symbol} due to missing overview.")
                continue

            row = {k: v for k, v in overview.items() if isinstance(v, (str, int, float)) or v is None}
            row["Last Updated"] = last_updated_map.get(symbol, "Unknown")

            prices = get_last_7_prices(symbol)
            for i, p in enumerate(prices):
                row[f"Price_Day_{i+1}"] = p

            if income_q:
                income_keys = ["totalRevenue", "grossProfit", "netIncome"]
                row.update(extract_quarterly(income_q, income_keys, "Income"))
            else:
                st.info(f"ℹ️ Income statement missing for {symbol}")

            if balance_q:
                balance_keys = ["totalAssets", "totalLiabilities", "totalShareholderEquity", "cashAndCashEquivalentsAtCarryingValue"]
                row.update(extract_quarterly(balance_q, balance_keys, "Balance"))
                try:
                    liabilities = float(row.get("Balance_totalLiabilities_Q1", 0))
                    equity = float(row.get("Balance_totalShareholderEquity_Q1", 1))
                    row["Debt to Equity Ratio"] = round(liabilities / equity, 2)
                except:
                    row["Debt to Equity Ratio"] = None
            else:
                st.info(f"ℹ️ Balance sheet missing for {symbol}")

            row["Is Complete"] = bool(income_q) and bool(balance_q)
            return row
        except Exception as e:
            st.error(f"Exception for {symbol}: {e}")
            time.sleep(1)
    return None

def get_full_data(symbols):
    all_data = []
    missing_symbols = []
    progress_bar = st.progress(0)
    total = len(symbols)
    completed = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            completed += 1
            progress_bar.progress(completed / total)
            try:
                result = future.result()
                if result:
                    all_data.append(result)
                else:
                    missing_symbols.append(futures[future])
            except Exception as e:
                missing_symbols.append(futures[future])
                st.error(f"Error processing {futures[future]}: {e}")

    st.success(f"✅ Returned {len(all_data)} valid rows")
    if missing_symbols:
        #st.warning(f"⚠️ Missing or filtered out: {', '.join(missing_symbols[:20])}... (Total: {len(missing_symbols)})")
        try:
            with open("missingsymbols.txt", "w") as f:
                f.write("\n".join(missing_symbols))
        except Exception as e:
            st.error(f"❌ Failed to write missing symbols: {e}")

    return pd.DataFrame(all_data)

# --- UI Section ---
st.markdown("""
    <h1 style='font-family: "Segoe UI", sans-serif; color: #1a1a1a; font-size: 42px; text-align: center; padding-bottom: 0.5rem;'>
        📊 <span style='color: #0056b3;'>US Stock Sector Analysis Dashboard*</span>
    </h1>
""", unsafe_allow_html=True)
st.markdown("""
<style>
    .main .block-container {
        padding-top: 1rem;
    }
    .st-emotion-cache-1y4p8pa {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 1rem;
    }
    .st-emotion-cache-1avcm0n, .st-emotion-cache-1p1jmeh {
        font-weight: 600;
        color: #2c3e50;
    }
</style>
""", unsafe_allow_html=True)


# Sidebar filters
with st.sidebar:
    st.image("https://img.icons8.com/ios-filled/50/funnel.png", width=30)
    st.markdown("### 🎛️ **Refine Your Dashboard**")
    st.caption("Use filters below to customize your view.")
    market_cap_category = st.selectbox("Market Cap Category", ["All", "Small Cap (<2B)", "Mid Cap (2B-10B)", "Large Cap (>10B)"])
    st.caption("💡 Categorized by total market capitalization in USD billions.")
    pe_max = st.number_input("Max PE Ratio", min_value=0.0, value=100.0)
    debt_to_equity_max = st.number_input("Max Debt to Equity Ratio", min_value=0.0, value=100.0)
    profit_margin_min = st.number_input("Min Profit Margin (%)", min_value=-100.0, value=0.0)
    gross_margin_min = st.number_input("Min Gross Margin (%)", min_value=-100.0, value=0.0)
    st.markdown("---")
    st.markdown("[Download Full Excel Below ⬇️](#-download-excel)")
    
full_df = get_full_data(all_symbols)
df = full_df.copy()

# Convert numeric fields to float where necessary
df["MarketCapitalization"] = pd.to_numeric(df["MarketCapitalization"], errors='coerce') / 1e9
df["PERatio"] = pd.to_numeric(df["PERatio"], errors='coerce')
df["Debt to Equity Ratio"] = pd.to_numeric(df["Debt to Equity Ratio"], errors='coerce')
df["ProfitMargin"] = pd.to_numeric(df["ProfitMargin"], errors='coerce') * 100
df["GrossProfitTTM"] = pd.to_numeric(df["GrossProfitTTM"], errors='coerce')/ 1e9
df = df.rename(columns={"MarketCapitalization": "Market Cap (Bn)", "PERatio": "PE Ratio", "ProfitMargin": "Profit Margin (%)"})

# Apply filters
if market_cap_category != "All":
    if market_cap_category == "Small Cap (<2B)":
        df = df[df["Market Cap (Bn)"] < 2]
    elif market_cap_category == "Mid Cap (2B-10B)":
        df = df[(df["Market Cap (Bn)"] >= 2) & (df["Market Cap (Bn)"] <= 10)]
    elif market_cap_category == "Large Cap (>10B)":
        df = df[df["Market Cap (Bn)"] > 10]

df = df[(df["PE Ratio"] <= pe_max) &
        (df["Debt to Equity Ratio"] <= debt_to_equity_max) &
        (df["Profit Margin (%)"] >= profit_margin_min)]

# Reorder columns to bring important fields next to Symbol
key_cols = [
    # Core Identifiers
    "Symbol", "Name",

    # Valuation Metrics
    "PE Ratio", "Market Cap (Bn)", "PriceToBookRatio",

    # Profitability Metrics
    "EPS", "EBITDA", "Profit Margin (%)", "OperatingMarginTTM", "GrossProfitTTM",

    # Leverage Metrics
    "Debt to Equity Ratio"
]
existing_cols = [col for col in key_cols if col in df.columns]
other_cols = [col for col in df.columns if col not in existing_cols]
df = df[existing_cols + other_cols]

# Apply icon-based column renaming and sorting for better UI
column_icons = {
    "PE Ratio": "📈 PE Ratio",
    "EPS": "💰 EPS",
    "EBITDA": "📊 EBITDA",
    "Debt to Equity Ratio": "🏦 Debt/Equity",
    "Market Cap (Bn)": "💼 Market Cap (B)",
    "Profit Margin (%)": "📉 Profit Margin (%)",
    "GrossProfitTTM": "📎 Gross Profit TTM",
    "OperatingMarginTTM": "📂 Operating Margin"
}
df.rename(columns=column_icons, inplace=True)
if df.empty:
    st.warning("No data found.")
    excel_buffer = io.BytesIO()
    pd.DataFrame().to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    st.download_button("📥 Download Excel", help="Download all available financial data for US stocks", data=excel_buffer, file_name="US_Stock_Financials.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    search = st.text_input("🔍 Search by Company or Ticker", placeholder="Type AAPL, Microsoft, etc.")
    if search:
        search_lower = search.lower()
        filtered_df = df[
            df["Name"].fillna("").str.lower().str.contains(search_lower) |
            df["Symbol"].fillna("").str.lower().str.contains(search_lower)
        ]
        st.write(f"📎 Showing {len(filtered_df)} results for '{search}'")
        st.dataframe(filtered_df, use_container_width=True)
    else:
        #st.write("📎 Showing all stocks")
        st.markdown("### 📊 Showing all US stocks")
      
st.dataframe(
    df.rename(columns={
        "PE Ratio": "📈 PE Ratio",
        "EPS": "💰 EPS",
        "EBITDA": "📊 EBITDA",
        "Debt to Equity Ratio": "🏦 Debt/Equity",
        "Market Cap (Bn)": "💼 Market Cap (B)",
        "Profit Margin (%)": "📉 Profit Margin (%)",
        "GrossProfitTTM": "📎 Gross Profit TTM"
    }),
    use_container_width=True
)

excel_buffer = io.BytesIO()
full_df.to_excel(excel_buffer, index=False, engine='openpyxl')
excel_buffer.seek(0)
st.download_button(
    "📥 Download Excel",
    data=excel_buffer,
    file_name="US_Stock_Financials.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
    

if "Sector" in df.columns:
    sectors = sorted(df['Sector'].dropna().unique())
    if sectors:
        tabs = st.tabs(sectors)
        for tab, sector in zip(tabs, sectors):
            with tab:
                st.subheader(f"📁 Sector Wise Data: {sector}")
                sector_df = df[df['Sector'] == sector]
                search = st.text_input(f"Search within {sector}", key=sector)
                if search:
                    sector_df = sector_df[
                        sector_df["Name"].str.contains(search, case=False, na=False) |
                        sector_df["Symbol"].str.contains(search, case=False, na=False)
                    ]
                st.dataframe(sector_df, use_container_width=True)
else:
    st.warning("No valid sector data found in the dataset.")
