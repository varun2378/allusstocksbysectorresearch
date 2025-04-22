import os
import glob
import pandas as pd
import requests
import time
import streamlit as st
import json
import hashlib

st.set_page_config(page_title="Sector-Wise Stock Financials", layout="wide")

API_KEY = "874WVCYGCPDM9YVS"
data_folder = r"./StockAnalysisData"

# Step 1: Read 2 companies per sector
sector_symbols_map = {}

excel_files = glob.glob(os.path.join(data_folder, "sp500_*_stocks.xlsx"))

for file in excel_files:
    try:
        df = pd.read_excel(file)
        sector = os.path.basename(file).replace("sp500_", "").replace("_stocks.xlsx", "").replace("_", " ")
        #load all symbols from the file
        symbols = df['Symbol'].dropna().tolist()
        #symbols = df['Symbol'].dropna().tolist()[:2]
        
        if symbols:
            sector_symbols_map[sector] = symbols
            print(f"✅ Loaded 2 symbols from {sector}: {symbols}")
    except Exception as e:
        print(f"⚠️ Skipping {file}: {e}")

# Flatten all symbols
all_symbols = [symbol for symbol_list in sector_symbols_map.values() for symbol in symbol_list]

# Alpha Vantage functions

# Create a persistent cache folder
cache_dir = r"./StockAnalysisData/api_cache"
os.makedirs(cache_dir, exist_ok=True)

@st.cache_data(ttl=86400)
def fetch_data(function, symbol):
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": API_KEY}
    
    # Generate a unique filename for this API request
    key_string = f"{function}_{symbol}"
    filename = os.path.join(cache_dir, hashlib.md5(key_string.encode()).hexdigest() + ".json")

    # If file exists, load from local cache
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            cached_data = json.load(f)
            return cached_data

    # Try fetching fresh data
    response = requests.get(url, params=params)
    try:
        data = response.json()
    except Exception as e:
        st.warning(f"⚠️ Error parsing response for {symbol}. Using cached version if available.")
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
        return {}

    # Check if we hit the API limit
    if "Information" in data and "rate limit" in data["Information"].lower():
        st.warning(f"⚠️ API rate limit hit for {symbol}. Using cached data.")
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
        return {}

    # Save to cache
    with open(filename, 'w') as f:
        json.dump(data, f)

    return data


@st.cache_data(ttl=86400)
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


@st.cache_data(ttl=86400)
def get_full_data(symbols):
    all_data = []
    for symbol in symbols:
        st.write(f"🔍 Processing `{symbol}`")
        overview = fetch_data("OVERVIEW", symbol)
        income_q = fetch_data("INCOME_STATEMENT", symbol)
        balance_q = fetch_data("BALANCE_SHEET", symbol)
        prices = get_last_7_prices(symbol)

        time.sleep(12)  # Respect rate limit

        row = {
            "Company Name": overview.get("Name", symbol),
            "Symbol": symbol,
            "Sector": overview.get("Sector"),
            "Industry": overview.get("Industry"),
            "Market Cap (USD Bn)": round(float(overview.get("MarketCapitalization", 0)) / 1e9, 2),
            "PE Ratio": overview.get("PERatio"),
            "EPS": overview.get("EPS"),
            "PEG": overview.get("PEGRatio"),
            "ProfitMargin": overview.get("ProfitMargin"),
            "OperatingMarginTTM": overview.get("OperatingMarginTTM"),
            "BookValue": overview.get("BookValue"),
            "PriceToBookRatio": overview.get("PriceToBookRatio")
        }

        for i, p in enumerate(prices):
            row[f"Price_Day_{i+1}"] = p

        # Add income statement
        income_keys = ["totalRevenue", "grossProfit", "netIncome"]
        row.update(extract_quarterly(income_q, income_keys, "Income"))

        # Add balance sheet
        balance_keys = ["totalAssets", "totalLiabilities", "totalShareholderEquity", "cashAndCashEquivalentsAtCarryingValue"]
        row.update(extract_quarterly(balance_q, balance_keys, "Balance"))

        # Add Debt to Equity Ratio
        try:
            liabilities = float(row.get("Balance_totalLiabilities_Q1", 0))
            equity = float(row.get("Balance_totalShareholderEquity_Q1", 1))  # avoid div by zero
            row["Debt to Equity Ratio"] = round(liabilities / equity, 2)
        except:
            row["Debt to Equity Ratio"] = None

        all_data.append(row)

    return pd.DataFrame(all_data)


# Step 2: Fetch all data
df = get_full_data(all_symbols)

# Step 3: Streamlit UI
st.title("📊 US Stocks – Sector-wise Financial Dashboard")

if df.empty:
    st.warning("No data found.")
else:
    # 🔍 Global Search Field
    search = st.text_input("🔍 Search Stock by Company Name or Symbol")

    # Apply Global Search Filter
    if search:
        search_lower = search.lower()
        filtered_df = df[
            df["Company Name"].fillna("").str.lower().str.contains(search_lower) |
            df["Symbol"].fillna("").str.lower().str.contains(search_lower)
        ]
        st.write(f"📎 Showing {len(filtered_df)} results for '{search}'")
        st.dataframe(filtered_df, use_container_width=True)
    else:
        st.write("📎 Showing all stocks")
        st.dataframe(df, use_container_width=True)
    
#else:
    # Get sectors from dataframe
    
if "Sector" in df.columns:
    sectors = sorted(df['Sector'].dropna().unique())
else:
    sectors = []

if sectors:
    tabs = st.tabs(sectors)
    for tab, sector in zip(tabs, sectors):
        with tab:
            st.subheader(f"📁 Sector: {sector}")
            sector_df = df[df['Sector'] == sector]

            # Search within sector
            search = st.text_input(f"Search within {sector}", key=sector)
            if search:
                sector_df = sector_df[
                    sector_df["Company Name"].str.contains(search, case=False, na=False) |
                    sector_df["Symbol"].str.contains(search, case=False, na=False)
                ]

            st.dataframe(sector_df, use_container_width=True)
else:
    st.warning("No valid sector data found in the dataset.")
