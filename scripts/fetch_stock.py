import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime
import os

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
}

def fetch_and_store(symbols_file="data/sp500_symbols.csv"):
    symbols = pd.read_csv(symbols_file)["Symbol"].tolist()
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol TEXT,
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            PRIMARY KEY(symbol, date)
        );
    """)
    
    for sym in symbols:
        print(f"Fetching {sym}...")
        df = yf.download(sym, period="5y", interval="1d")
        df.reset_index(inplace=True)
        
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (sym, row['Date'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Historical data loaded successfully.")

if __name__ == "__main__":
    fetch_and_store()

