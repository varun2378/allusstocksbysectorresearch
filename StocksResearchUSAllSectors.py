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

API_KEY = "QLSJQ"
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
    except Exception as e:
        print(f"⚠️ Skipping {file}: {e}")

all_symbols = [symbol for symbol_list in sector_symbols_map.values() for symbol in symbol_list]

refresh_data = st.sidebar.checkbox("🔁 Force Refresh (Ignore Cache)", value=False)
min_market_cap = st.sidebar.number_input("🔍 Filter by Min Market Cap (Bn)", value=0.0, step=0.1)
pe_ratio_filter = st.sidebar.number_input("📊 Max PE Ratio (0 to ignore)", value=0.0, step=0.1)
profit_margin_filter = st.sidebar.number_input("💰 Min Profit Margin (0 to ignore)", value=0.0, step=0.1)
eps_filter = st.sidebar.number_input("📈 Min EPS (0 to ignore)", value=0.0, step=0.1)
peg_filter = st.sidebar.number_input("📉 Max PEG (0 to ignore)", value=0.0, step=0.1)
debt_to_equity_max = st.sidebar.number_input("⚖️ Max Debt to Equity Ratio (0 to ignore)", value=0.0, step=0.1)
min_ebitda = st.sidebar.number_input("🏦 Min EBITDA (Bn)", value=0.0, step=0.1, help="Earnings before interest, taxes, depreciation, and amortization")
min_gross_profit_ttm = st.sidebar.number_input("💹 Min Gross Profit TTM (Bn)", value=0.0, step=0.1)

@st.cache_data(ttl=86400)
def fetch_data(function, symbol):
    key_string = f"{function}_{symbol}"
    filename = os.path.join(cache_dir, hashlib.md5(key_string.encode()).hexdigest() + ".json")
    if not refresh_data and os.path.exists(filename):
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
    try:
        response = requests.get(url, params=params)
        data = response.json()
    except:
        return {}
    if "Information" in data and "limit" in data["Information"].lower():
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
        return {}
    try:
        with open(filename, 'w') as f:
            json.dump(data, f)
        last_updated_map[symbol] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except:
        pass
    return data

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

def passes_filters(result):
    return (
        result["Market Cap (USD Bn)"] >= min_market_cap and
        (pe_ratio_filter == 0 or result["PE Ratio"] <= pe_ratio_filter) and
        (profit_margin_filter == 0 or result["ProfitMargin"] >= profit_margin_filter) and
        (eps_filter == 0 or result["EPS"] >= eps_filter) and
        (peg_filter == 0 or result["PEG"] <= peg_filter) and
        (debt_to_equity_max == 0 or (
            result.get("Debt to Equity Ratio") is not None and
            result["Debt to Equity Ratio"] <= debt_to_equity_max
        )) and
        (min_ebitda == 0 or result.get("EBITDA (Bn)", 0) >= min_ebitda) and
        (min_gross_profit_ttm == 0 or result.get("Gross Profit TTM (Bn)", 0) >= min_gross_profit_ttm)
    )

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
        "Last Updated": last_updated_map.get(symbol, "Unknown")
    }
    for i, p in enumerate(prices):
        row[f"Price_Day_{i+1}"] = p
    row.update({k: (float(v)/1e6 if v not in [None, 'None'] else None) for k, v in extract_quarterly(income_q, ["totalRevenue", "grossProfit", "netIncome"], "Income").items()})
    row.update({k: (float(v)/1e6 if v not in [None, 'None'] else None) for k, v in extract_quarterly(balance_q, ["totalAssets", "totalLiabilities", "totalShareholderEquity", "cashAndCashEquivalentsAtCarryingValue"], "Balance").items()})
    try:
        liabilities = float(row.get("Balance_totalLiabilities_Q1", 0))
        equity = float(row.get("Balance_totalShareholderEquity_Q1", 1))
        row["Debt to Equity Ratio"] = round(liabilities / equity, 2)
    except:
        row["Debt to Equity Ratio"] = None
    return row

def get_full_data(symbols):
    all_data = []
    progress = st.progress(0)
    max_workers = 10
    completed = 0
    total = len(symbols)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                result = future.result()
                if passes_filters(result):
                    all_data.append(result)
            except Exception as e:
                st.error(f"Error processing {futures[future]}: {e}")
            completed += 1
            progress.progress(completed / total)
    progress.empty()
    return pd.DataFrame(all_data)

df = get_full_data(all_symbols)
st.title("📊 US Stocks – Financial Dashboard")
if df.empty:
    st.warning("No data found.")
else:
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download as CSV", data=csv, file_name="US_Stocks_Financials.csv", mime="text/csv")
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
    st.download_button("📘 Download as Excel", data=excel_buffer.getvalue(), file_name="US_Stocks_Financials.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if "Sector" in df.columns:
        sectors = sorted(df['Sector'].dropna().unique())
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
        st.dataframe(df)
