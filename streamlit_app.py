# streamlit_app.py
import streamlit as st
import os
from scripts.embeddings_vector_db import create_or_update_embeddings, load_vectorstore
from scripts.fetch_update_daily import main as run_fetch_update

st.set_page_config(page_title="S&P500 LLM Chatbot", layout="wide")
st.title("📈 S&P 500 LLM Chatbot")

st.markdown("Ask historical price questions (e.g., `What's AAPL close on 2022-08-05?`)")

# Buttons for manual operations
col1, col2 = st.columns(2)
with col1:
    if st.button("Fetch & update stock data (manual)"):
        with st.spinner("Fetching and updating DB..."):
            run_fetch_update()
        st.success("Database updated.")

with col2:
    if st.button("(Re)build embeddings (manual)"):
        with st.spinner("Building embeddings (may take a few minutes)..."):
            create_or_update_embeddings()
        st.success("Embeddings created/updated.")

# Ensure embeddings exist at startup (safe no-op if already created)
if not os.path.exists("vector_store"):
    with st.spinner("Creating embeddings on first run..."):
        create_or_update_embeddings()

# Load vectorstore & LLM
from langchain.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain.chat_models import ChatOpenAI
from langchain.chains import RetrievalQA

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
vectordb = Chroma(persist_directory="vector_store", embedding_function=embeddings)
retriever = vectordb.as_retriever(search_kwargs={"k": 6})
llm = ChatOpenAI(temperature=0, openai_api_key=OPENAI_API_KEY)
qa = RetrievalQA.from_chain_type(llm=llm, retriever=retriever)

query = st.text_input("Ask a question about stock prices or HQ data:")
if query:
    with st.spinner("Thinking..."):
        answer = qa.run(query)
    st.markdown("**Answer:**")
    st.write(answer)

