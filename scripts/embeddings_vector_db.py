import os
import pickle
import psycopg2
import pandas as pd
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
}

VECTOR_PATH = "vector_store.pkl"

def create_embeddings():
    # Check if vector DB already exists
    if os.path.exists(VECTOR_PATH):
        print("✅ Vector DB already exists. Skipping creation.")
        return

    print("📦 Creating vector DB embeddings...")

    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM stock_data ORDER BY date", conn)
    conn.close()

    # Prepare text for embeddings
    texts = [f"{row['symbol']} {row['date']} close:{row['close']}" for _, row in df.iterrows()]

    embeddings = OpenAIEmbeddings(openai_api_key=os.environ["OPENAI_API_KEY"])
    vector_store = FAISS.from_texts(texts, embeddings)

    # Save vector DB locally
    with open(VECTOR_PATH, "wb") as f:
        pickle.dump(vector_store, f)

    print("✅ Embeddings created and stored successfully.")

if __name__ == "__main__":
    create_embeddings()
