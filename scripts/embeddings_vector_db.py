import os
import pandas as pd
import psycopg2
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI

def get_db_connection(DB_CONFIG):
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
    return conn

def get_vectorstore(DB_CONFIG, OPENAI_API_KEY, hq_df):
    # Connect to DB and fetch stock data
    conn = get_db_connection(DB_CONFIG)
    stock_df = pd.read_sql("SELECT * FROM stock_data ORDER BY date", conn)
    conn.close()
    
    # Combine stock + HQ for embedding
    combined_df = stock_df.merge(hq_df, left_on="symbol", right_on="Symbol", how="left")
    combined_texts = combined_df.apply(lambda x: f"{x['symbol']} on {x['date']}: Open={x['open']}, Close={x['close']}, HQ={x['City']}, {x['Address']}", axis=1).tolist()
    
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    vectorstore = FAISS.from_texts(combined_texts, embeddings)
    return vectorstore

def query_vectorstore(vectorstore, query):
    qa_chain = RetrievalQA.from_chain_type(
        llm=OpenAI(openai_api_key=os.environ["OPENAI_API_KEY"], temperature=0),
        chain_type="stuff",
        retriever=vectorstore.as_retriever()
    )
    result = qa_chain.run(query)
    return result

