# scripts/embeddings_vector_db.py
import os
import pandas as pd
from langchain.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from sqlalchemy import create_engine, text

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True)

PERSIST_DIR = "vector_store"

def create_or_update_embeddings():
    # Load stock_data from DB
    with engine.begin() as conn:
        df = pd.read_sql(text("SELECT symbol, date, open, high, low, close, volume FROM stock_data ORDER BY date"), conn)

    if df.empty:
        print("No stock data found to create embeddings.")
        return

    # Create text representation (one per row)
    texts = df.apply(lambda r: f"{r.symbol} on {r.date}: open={r.open}, high={r.high}, low={r.low}, close={r.close}, volume={r.volume}", axis=1).tolist()
    metadatas = df.apply(lambda r: {"symbol": r.symbol, "date": str(r.date)}, axis=1).tolist()

    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    vectordb = Chroma.from_texts(texts, embeddings, metadatas=metadatas, persist_directory=PERSIST_DIR)
    vectordb.persist()
    print(f"Chroma vector DB created/persisted at {PERSIST_DIR}")

def load_vectorstore():
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    return Chroma(persist_directory=PERSIST_DIR, embedding_function=embeddings)


