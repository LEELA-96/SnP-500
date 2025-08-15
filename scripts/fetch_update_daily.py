# scripts/fetch_update_daily.py
import os
import pandas as pd
import yfinance as yf
import psycopg2
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# DB config from env
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True)

def ensure_tables():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                PRIMARY KEY (symbol, date)
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS company_hq (
                symbol TEXT PRIMARY KEY,
                company_name TEXT,
                city TEXT,
                address TEXT
            );
        """))

def get_sp500_symbols():
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    df = pd.read_csv(url)
    # Ensure Symbol column exists
    if "Symbol" not in df.columns:
        raise RuntimeError("S&P symbols CSV missing 'Symbol' column")
    return df["Symbol"].astype(str).tolist()

def fetch_stock_data(symbol, start_date, end_date):
    try:
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)
        if df.empty:
            return pd.DataFrame()
        df = df.reset_index()
        df = df.rename(columns={"Date": "date", "Open":"open", "High":"high","Low":"low","Close":"close","Volume":"volume"})
        df["symbol"] = symbol
        return df[["symbol","date","open","high","low","close","volume"]]
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.DataFrame()

def upsert_stock_df(df):
    if df.empty:
        return 0
    # Use pandas.to_sql with if_exists='append', but dedupe via ON CONFLICT using raw insert for few rows
    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = text("""
                INSERT INTO stock_data (symbol,date,open,high,low,close,volume)
                VALUES (:symbol,:date,:open,:high,:low,:close,:volume)
                ON CONFLICT (symbol, date) DO NOTHING
            """)
            conn.execute(stmt, {
                "symbol": row["symbol"],
                "date": row["date"].date() if hasattr(row["date"], "date") else str(row["date"]),
                "open": float(row["open"]) if pd.notna(row["open"]) else None,
                "high": float(row["high"]) if pd.notna(row["high"]) else None,
                "low": float(row["low"]) if pd.notna(row["low"]) else None,
                "close": float(row["close"]) if pd.notna(row["close"]) else None,
                "volume": int(row["volume"]) if pd.notna(row["volume"]) else None
            })
            inserted += 1
    return inserted

def load_hq_from_excel(path="data/Company_S&HQS (1).xlsx"):
    if not os.path.exists(path):
        print("HQ file not found at", path)
        return
    df = pd.read_excel(path)
    # expected columns: Symbol, Company, City, Address (adjust if different)
    colmap = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in ("symbol",):
            colmap[c] = "symbol"
        elif lc in ("company","companyname","company name","name"):
            colmap[c] = "company_name"
        elif lc in ("city",):
            colmap[c] = "city"
        elif lc in ("address","addr"):
            colmap[c] = "address"
    df = df.rename(columns=colmap)
    required = ["symbol"]
    if "symbol" not in df.columns:
        raise RuntimeError("HQ excel missing 'Symbol' column")
    # Upsert into company_hq
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = text("""
                INSERT INTO company_hq (symbol, company_name, city, address)
                VALUES (:symbol, :company_name, :city, :address)
                ON CONFLICT (symbol) DO UPDATE
                SET company_name = EXCLUDED.company_name,
                    city = EXCLUDED.city,
                    address = EXCLUDED.address;
            """)
            conn.execute(stmt, {
                "symbol": str(row.get("symbol")),
                "company_name": row.get("company_name"),
                "city": row.get("city"),
                "address": row.get("address")
            })
    print("HQ loaded/updated")

def main():
    print("Ensuring tables exist...")
    ensure_tables()

    symbols = get_sp500_symbols()
    print(f"Found {len(symbols)} symbols. Starting fetch...")

    # Determine fetch window: if table empty -> 5 years, else backfill from latest date
    with engine.begin() as conn:
        res = conn.execute(text("SELECT MAX(date) as max_date FROM stock_data;"))
        row = res.fetchone()
        max_date = row["max_date"] if row is not None else None

    today = datetime.utcnow().date()
    if max_date is None:
        start_date = today - timedelta(days=365*5)
    else:
        start_date = (max_date + timedelta(days=1))

    print("Fetching from", start_date, "to", today)

    for i, symbol in enumerate(symbols, 1):
        try:
            print(f"[{i}/{len(symbols)}] {symbol} ...")
            df = fetch_stock_data(symbol, start_date.strftime("%Y-%m-%d"), (today + timedelta(days=1)).strftime("%Y-%m-%d"))
            if not df.empty:
                inserted = upsert_stock_df(df)
                print(f"  inserted approx {inserted} rows")
            else:
                print("  no data")
        except Exception as e:
            print("  failed:", e)

    # Load HQ table
    load_hq_from_excel()

if __name__ == "__main__":
    main()
