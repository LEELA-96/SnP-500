import os
import shutil
import psycopg2
import pandas as pd
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
}

VECTOR_PATH = "vector_store"

def create_embeddings():
    # Delete old vector DB to force rebuild if needed
    if os.path.exists(VECTOR_PATH):
        shutil.rmtree(VECTOR_PATH)

    print("📦 Creating vector DB embeddings...")

    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM stock_data ORDER BY date", conn)
    conn.close()

    texts = [f"{row['symbol']} {row['date']} close:{row['close']}" for _, row in df.iterrows()]

    embeddings = OpenAIEmbeddings(openai_api_key=os.environ["OPENAI_API_KEY"])
    vector_store = Chroma.from_texts(texts, embeddings, persist_directory=VECTOR_PATH)
    vector_store.persist()

    print("✅ Embeddings created and stored successfully.")

if __name__ == "__main__":
    create_embeddings()
