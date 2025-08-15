from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
import psycopg2
import pandas as pd
import os
import pickle

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
}

def create_embeddings():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM stock_data ORDER BY date", conn)
    conn.close()
    
    # Create embeddings for stock data (symbol + date + close)
    texts = [f"{row['symbol']} {row['date']} close:{row['close']}" for _, row in df.iterrows()]
    
    embeddings = OpenAIEmbeddings(openai_api_key=os.environ["OPENAI_API_KEY"])
    vector_store = FAISS.from_texts(texts, embeddings)
    
    # Save vector DB locally
    with open("vector_store.pkl", "wb") as f:
        pickle.dump(vector_store, f)
    
    print("✅ Embeddings created and stored locally.")

if __name__ == "__main__":
    create_embeddings()

