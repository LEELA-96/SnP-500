import pandas as pd
import psycopg2
import os

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
}

def load_hq(file="data/Company_S&HQS (1).xlsx"):
    df = pd.read_excel(file)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS headquarters_history (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            city TEXT,
            address TEXT
        );
    """)
    
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO headquarters_history (symbol, company_name, city, address)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT(symbol) DO UPDATE
            SET company_name=EXCLUDED.company_name,
                city=EXCLUDED.city,
                address=EXCLUDED.address;
        """, (row['Symbol'], row['Company'], row['City'], row['Address']))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Headquarters data loaded successfully.")

if __name__ == "__main__":
    load_hq()

