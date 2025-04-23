import os
import glob
import pandas as pd
import requests
import time
import streamlit as st
import json
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from openpyxl import Workbook

st.set_page_config(page_title="Sector-Wise Stock Financials", layout="wide")

API_KEY = "QLSJQLW02XODEZUS"
data_folder = r"./StockAnalysisData"
cache_dir = r"./StockAnalysisData/api_cache"
os.makedirs(cache_dir, exist_ok=True)

sector_symbols_map = {}
excel_files = glob.glob(os.path.join(data_folder, "sp500_*_stocks.xlsx"))
for file in excel_files:
    try:
        df = pd.read_excel(file)
        sector = os.path.basename(file).replace("sp500_", "").replace("_stocks.xlsx", "").replace("_", " ")
        symbols = df['Symbol'].dropna().tolist()
        if symbols:
            sector_symbols_map[sector] = symbols
    except Exception as e:
        print(f"⚠️ Skipping {file}: {e}")

all_symbols = [symbol for symbol_list in sector_symbols_map.values() for symbol in symbol_list]

refresh_data = st.sidebar.checkbox("🔁 Force Refresh (Ignore Cache)", value=False)
min_market_cap = st.sidebar.number_input("Min Market Cap (Bn)", value=0.0)
pe_ratio_filter = st.sidebar.number_input("Max PE Ratio", value=0.0)
profit_margin_filter = st.sidebar.number_input("Min Profit Margin", value=0.0)
eps_filter = st.sidebar.number_input("Min EPS", value=0.0)
peg_filter = st.sidebar.number_input("Max PEG", value=0.0)
debt_to_equity_max = st.sidebar.number_input("Max Debt to Equity Ratio", value=0.0)
min_ebitda = st.sidebar.number_input("Min EBITDA (Bn)", value=0.0)
min_gross_profit_ttm = st.sidebar.number_input("Min Gross Profit TTM (Bn)", value=0.0)

@st.cache_data(ttl=86400)
def fetch_data(function, symbol):
    key_string = f"{function}_{symbol}"
    filename = os.path.join(cache_dir, hashlib.md5(key_string.encode()).hexdigest() + ".json")
    if not refresh_data and os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": API_KEY}
    try:
        response = requests.get(url, params=params)
        if 'Information' in response.json() and 'Please contact premium' in response.json()['Information']:
            st.warning(f"🚫 Premium API warning for {symbol}. Retrying after 1 second...")
            time.sleep(1)
            response = requests.get(url, params=params)
        data = response.json()
        with open(filename, 'w') as f:
            json.dump(data, f)
        return data
    except:
        return {}

def get_last_7_prices(symbol):
    data = fetch_data("TIME_SERIES_DAILY_ADJUSTED", symbol)
    ts = data.get("Time Series (Daily)", {})
    return [float(ts[date]["5. adjusted close"]) for date in sorted(ts.keys(), reverse=True)[:7] if "5. adjusted close" in ts[date]]

def extract_quarterly(data, keys, prefix):
    result = {}
    if "quarterlyReports" in data:
        for i, report in enumerate(data["quarterlyReports"][:4]):
            for key in keys:
                col = f"{prefix}_{key}_Q{i+1}"
                result[col] = report.get(key)
    return result

def passes_filters(row):
    try:
        return (
            row["Market Cap (USD Bn)"] >= min_market_cap and
            (pe_ratio_filter == 0 or row["PE Ratio"] <= pe_ratio_filter) and
            (profit_margin_filter == 0 or row["ProfitMargin"] >= profit_margin_filter) and
            (eps_filter == 0 or row["EPS"] >= eps_filter) and
            (peg_filter == 0 or row["PEG"] <= peg_filter) and
            (debt_to_equity_max == 0 or row.get("Debt to Equity Ratio") is not None and row["Debt to Equity Ratio"] <= debt_to_equity_max) and
            (min_ebitda == 0 or row.get("EBITDA (Bn)", 0) >= min_ebitda) and
            (min_gross_profit_ttm == 0 or row.get("Gross Profit TTM (Bn)", 0) >= min_gross_profit_ttm)
        )
    except:
        return False

def process_symbol(symbol):
    overview = fetch_data("OVERVIEW", symbol)
    income_q = fetch_data("INCOME_STATEMENT", symbol)
    balance_q = fetch_data("BALANCE_SHEET", symbol)
    prices = get_last_7_prices(symbol)
    try:
        market_cap = float(overview.get("MarketCapitalization", 0)) / 1e9
        pe_ratio = float(overview.get("PERatio", 0))
        profit_margin = float(overview.get("ProfitMargin", 0)) * 100
        eps = float(overview.get("EPS", 0))
        peg = float(overview.get("PEGRatio", 0))
        ebitda = float(overview.get("EBITDA", 0)) / 1e9
        gross_profit_ttm = float(overview.get("GrossProfitTTM", 0)) / 1e9
        book_value = float(overview.get("BookValue", 0))
    except:
        market_cap = pe_ratio = profit_margin = eps = peg = ebitda = gross_profit_ttm = book_value = 0
    row = {
        "Company Name": overview.get("Name", symbol),
        "Symbol": symbol,
        "Sector": overview.get("Sector"),
        "Industry": overview.get("Industry"),
        "Market Cap (USD Bn)": market_cap,
        "Debt to Equity Ratio": None,
        "PE Ratio": pe_ratio,
        "EPS": eps,
        "PEG": peg,
        "ProfitMargin": profit_margin,
        "BookValue": book_value,
        "PriceToBookRatio": overview.get("PriceToBookRatio"),
        "EBITDA (Bn)": ebitda,
        "Gross Profit TTM (Bn)": gross_profit_ttm,
    }
    for i, p in enumerate(prices):
        row[f"Price_Day_{i+1}"] = p
    row.update({k: float(v)/1e6 if v not in [None, 'None'] else None for k, v in extract_quarterly(income_q, ["totalRevenue", "grossProfit", "netIncome"], "Income").items()})
    row.update({k: float(v)/1e6 if v not in [None, 'None'] else None for k, v in extract_quarterly(balance_q, ["totalAssets", "totalLiabilities", "totalShareholderEquity", "cashAndCashEquivalentsAtCarryingValue"], "Balance").items()})
    try:
        liabilities = float(row.get("Balance_totalLiabilities_Q1", 0))
        equity = float(row.get("Balance_totalShareholderEquity_Q1", 1))
        row["Debt to Equity Ratio"] = round(liabilities / equity, 2)
    except:
        row["Debt to Equity Ratio"] = None
    return row

def get_full_data(symbols):
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                result = future.result()
                if passes_filters(result):
                    all_data.append(result)
            except Exception as e:
                st.error(f"Error processing {futures[future]}: {e}")
    return pd.DataFrame(all_data)

@st.cache_data(ttl=86400)
def load_sector_jsons():
    combined_data = []
    for file in os.listdir(cache_dir):
        if file.startswith("sector_") and file.endswith(".json"):
            try:
                with open(os.path.join(cache_dir, file), 'r') as f:
                    records = json.load(f)
                    combined_data.extend(records)
            except Exception as e:
                st.warning(f"Could not load {file}: {e}")
    return pd.DataFrame(combined_data)

if not refresh_data:
    df = load_sector_jsons()
    if df.empty:
        df = get_full_data(all_symbols)
else:
    df = get_full_data(all_symbols)

# Save per-sector cache
for sector, symbols in sector_symbols_map.items():
    sector_df = df[df['Symbol'].isin(symbols)]
    if not sector_df.empty:
        sector_file = os.path.join(cache_dir, f"sector_{sector.replace(' ', '_')}.json")
        with open(sector_file, 'w') as f:
            json.dump(sector_df.to_dict(orient='records'), f, indent=2)

# Show dashboard
st.title("📊 US Stocks – Financial Dashboard")
if df.empty:
    st.warning("No data found.")
else:
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download as CSV", data=csv, file_name="US_Stocks_Financials.csv", mime="text/csv")
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
    st.download_button("📘 Download as Excel", data=excel_buffer.getvalue(), file_name="US_Stocks_Financials.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    sectors = sorted(df['Sector'].dropna().unique())
    if sectors:
        tabs = st.tabs(sectors)
        for tab, sector in zip(tabs, sectors):
            with tab:
                st.subheader(f"📁 Sector: {sector}")
                sector_df = df[df['Sector'] == sector]
                search = st.text_input(f"Search in {sector}", key=sector)
                if search:
                    sector_df = sector_df[
                        sector_df["Company Name"].str.contains(search, case=False, na=False) |
                        sector_df["Symbol"].str.contains(search, case=False, na=False)
                    ]
                st.dataframe(sector_df, use_container_width=True)
    else:
        st.warning("No sectors found in the data.")
