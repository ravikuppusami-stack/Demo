import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import gspread
from google.oauth2.service_account import Credentials
from google import genai

# Configurations
DATABASE_URL = 'mysql+pymysql://root:Password@localhost/working_file'  # Update password
SERVICE_ACCOUNT_FILE = 'focus-chain-469912-s3-cf8933ca20e7.json'
GOOGLE_SHEET_NAME = "Testing"
GEMINI_API_KEY = "AIzaSyCsGl_MjnKvY2Z7Lyf33CZfRaSndyiXLsQ"

# Setup clients
engine = create_engine(DATABASE_URL)
client = genai.Client(api_key=GEMINI_API_KEY)

def get_gsheet_dataframe(worksheet_name):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client_gs = gspread.authorize(creds)
    sheet = client_gs.open(GOOGLE_SHEET_NAME).worksheet(worksheet_name)
    records = sheet.get_all_records()
    return pd.DataFrame(records)

def get_full_schema_info():
    conn = pymysql.connect(host='localhost', user='root', password='Password', db='working_file')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """, ('working_file',))
    tables = {}
    for table, column, col_type in cursor.fetchall():
        tables.setdefault(table, []).append(f"{column} {col_type}")
    cursor.close()
    conn.close()
    lines = ["Tables:"]
    for table, cols in tables.items():
        lines.append(f"- {table}({', '.join(cols)})")
    return "\n".join(lines)

def clean_sql_query(sql_query: str) -> str:
    if not isinstance(sql_query, str) or not sql_query:
        return ""
    sql_query = sql_query.strip()
    if sql_query.startswith("```"):
        parts = sql_query.split("```")
        for part in parts[1:]:
            if part.strip():
                sql_block = part.strip()
                if sql_block.lower().startswith("sql"):
                    sql_block = sql_block[3:].lstrip(" \n")
                return sql_block
        return ""
    if sql_query.lower().startswith("sql"):
        sql_query = sql_query[3:].lstrip(" \n")
    return sql_query

def generate_sql_from_prompt(prompt: str, schema: str) -> str:
    full_prompt = f"""
You are a MySQL SQL expert with this database schema:

{schema}

User request: "{prompt}"

Write SQL that joins the 'loan' table with two sheets from the Google Sheet:
- 'Mapping' worksheet with columns brand_name and spoc_name
- 'Target' worksheet with columns spoc_name and target

Group results by spoc_name and return columns: spoc_name, target sum, and loan_amount sum (converted to crores).

Order by spoc_name ascending. Return only valid SQL, no explanation.
"""
    try:
        response = client.models.generate_content(
            model="gemini-1.5-flash",
            contents=full_prompt
        )
        if not hasattr(response, "text") or not response.text:
            return ""
        return response.text.strip()
    except Exception as e:
        st.error(f"Gemini API Error: {e}. Please retry later.")
        return ""

def query_db(sql_query: str):
    sql = clean_sql_query(sql_query)
    if not sql:
        return pd.DataFrame()
    try:
        df = pd.read_sql(text(sql), engine)
        return df
    except Exception as e:
        st.error(f"SQL Execution Error: {e}")
        return pd.DataFrame()

st.title("AI SQL with Gemini - SPOC Loan and Target")

user_input = st.text_input("Enter your question for the database:")

if user_input:
    # Load Google Sheet sheets info and show shape
    try:
        mapping_df = get_gsheet_dataframe('Mapping')
        tar_df = get_gsheet_dataframe('Tar')
        st.write(f"Mapping DataFrame: {mapping_df.shape}")
        st.write(f"Tar DataFrame: {tar_df.shape}")
    except Exception as e:
        st.error(f"Google Sheet loading error: {e}")
        st.stop()

    # Get DB schema info
    db_schema = get_full_schema_info()

    # Generate SQL from LLM
    ai_sql = generate_sql_from_prompt(user_input, db_schema)
    if not ai_sql:
        st.error("No SQL generated from Gemini. Try again later.")
        st.stop()

    st.code(ai_sql, language="sql")

    # Query MySQL with generated SQL
    result_df = query_db(ai_sql)

    if result_df.empty:
        st.warning("No results found or query execution failed. Check Google Sheets import to MySQL or query correctness.")
    else:
        st.dataframe(result_df)

        # Download CSV