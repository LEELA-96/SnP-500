import streamlit as st
import os
import pandas as pd
import psycopg2
from datetime import datetime
from embeddings_vector_db import get_vectorstore, query_vectorstore

# --- Database Config from Streamlit Secrets ---
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ["DB_PORT"]),
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
}

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# --- Streamlit UI ---
st.title("📈 S&P 500 LLM Chatbot")

st.markdown("""
Ask questions about S&P 500 stock prices and company info. 
Example: "What's Apple's price on August 5th, 2022?"
""")

user_query = st.text_input("Enter your question:")

# --- Load HQ data (already uploaded in repo) ---
hq_df = pd.read_excel("data/Company_S&HQ (1).xlsx")

# --- Vectorstore setup ---
vectorstore = get_vectorstore(DB_CONFIG, OPENAI_API_KEY, hq_df)

if st.button("Ask"):
    if user_query.strip() == "":
        st.warning("Please enter a question.")
    else:
        result = query_vectorstore(vectorstore, user_query)
        st.write(result)
