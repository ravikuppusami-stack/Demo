# pip install langchain langgraph langchain_openai sqlalchemy langchain_community mysqlclient

import os
from sqlalchemy import create_engine
from langchain.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv

load_dotenv()


DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

engine = create_engine(DATABASE_URL)
db = SQLDatabase(engine)
llm = ChatGoogleGenerativeAI(google_api_key=GOOGLE_API_KEY, model="gemini-2.5-flash")
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()

system_prompt = """
You are an agent designed to interact with a MySQL database.
Given an input question, create a syntactically correct MySQL query, check the query, and run it.
Start by inspecting available tables and relevant schemas, then answer using the query result.
Never perform DML (insert/update/delete) operations. If you get any error, revise and retry.
"""

agent = create_react_agent(llm, tools, prompt=system_prompt)

def main():
    print("Chat with your MySQL database (Ctrl+C to quit): ")
    while True:
        natural_question = input("> ")
        for step in agent.stream(
            {"messages": [{"role": "user", "content": natural_question}]},
            stream_mode="values"
        ):
            msg = step["messages"][-1]
            try:
                print(msg.pretty_print())
            except Exception:
                print(msg.content)

if __name__ == "__main__":
    main()
