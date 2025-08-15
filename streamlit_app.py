import streamlit as st
import pickle
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from langchain.chains import RetrievalQA
from langchain.chat_models import ChatOpenAI
import os

st.title("📈 S&P 500 Chatbot")

# Load vector DB
with open("vector_store.pkl", "rb") as f:
    vector_store = pickle.load(f)

qa = RetrievalQA.from_chain_type(
    llm=ChatOpenAI(temperature=0, openai_api_key=os.environ["OPENAI_API_KEY"]),
    retriever=vector_store.as_retriever()
)

query = st.text_input("Ask me about any stock (e.g., 'Apple price on 2022-08-05'):")

if query:
    response = qa.run(query)
    st.write("💬", response)

