# Requirements:
# pip install streamlit langchain langchain_community langchain_openai langgraph sqlalchemy mysqlclient python-dotenv

import os
import streamlit as st
from sqlalchemy import create_engine
from langchain.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv

# Load your environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Fail early if env vars missing
if not DATABASE_URL or not OPENAI_API_KEY:
    st.error("Please set DATABASE_URL and OPENAI_API_KEY in your .env file.")
    st.stop()

# SQLAlchemy + LangChain DB
engine = create_engine(DATABASE_URL)
db = SQLDatabase(engine)

# LangChain OpenAI LLM
llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY, model="gpt-3.5-turbo")

# Toolkit and tools
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()

# System prompt—safe and robust!
system_prompt = """
You are an agent designed to interact with a MySQL database.
Given an input question, create a syntactically correct MySQL query, check the query, and run it.
Start by inspecting available tables and relevant schemas, then answer using the query result.
Never perform DML (insert/update/delete) operations. If you get any error, revise and retry.
"""
# Create LangGraph ReAct agent
agent = create_react_agent(llm, tools, prompt=system_prompt)

# --- Streamlit UI ---
st.title("SQL Agent")

if 'history' not in st.session_state:
    st.session_state['history'] = []

user_input = st.text_input("Ask your database a question:", key="query")
submit = st.button("Submit", key="submit_button")

if submit:
    if user_input.strip() == "":
        st.warning("Please enter a question.")
    else:
        st.session_state['history'].append({"role": "user", "content": user_input})
        with st.spinner("Thinking..."):
            try:
                result = agent.invoke({"messages": st.session_state['history']})
                final_msg = result["messages"][-1]
                st.session_state['history'].append({"role": "assistant", "content": final_msg.content})
                st.markdown(final_msg.content)
            except Exception as e:
                st.error(f"Error: {e}")

if len(st.session_state['history']) > 0:
    with st.expander("Show conversation history"):
        for msg in st.session_state['history']:
            if msg["role"] == "user":
                st.markdown(f"**User:** {msg['content']}")
            else:
                st.markdown(f"**Assistant:** {msg['content']}")
