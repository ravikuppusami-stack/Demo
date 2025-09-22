import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from google import genai
import pymysql
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable missing or empty.")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable missing or empty.")

engine = create_engine(DATABASE_URL)
client = genai.Client(api_key=GEMINI_API_KEY)

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
    schema_lines = ["Tables:"]
    for table, cols in tables.items():
        cols_str = ", ".join(cols)
        schema_lines.append(f"- {table}({cols_str})")
    return "\n".join(schema_lines)

def clean_sql_query(sql_query: str) -> str:
    sql = sql_query.strip()
    if sql.startswith("```"):
        parts = sql.split("```")
        sql = parts[1] if len(parts) > 1 else parts[0]
    if sql.lower().startswith("sql"):
        sql = sql[3:].lstrip(" \n")
    return sql

def generate_sql_from_prompt(prompt: str, schema: str) -> str:
    full_prompt = f"""
You are an expert SQL assistant for a MySQL database with this schema:

{schema}

User request: "{prompt}"

Write a valid MySQL SQL query ONLY to answer this. 
When aggregating sums, use alias 'total_loan_amount' instead of 'values'.
If your query includes aggregate functions like SUM or COUNT, make sure to include a proper GROUP BY clause for all other selected columns or expressions.
Avoid joins or selections that cause duplicate loan_amount entries.
Make sure to JOIN institute table to get spoc_name mapped to loans.
"""
    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=full_prompt
    )
    return response.text.strip()

def query_db(sql_query: str):
    sql = clean_sql_query(sql_query)
    try:
        df = pd.read_sql(text(sql), engine)
        return df
    except Exception as e:
        return f"SQL Execution Error: {e}"

def convert_to_crores(amount):
    crores = 10000000
    try:
        if pd.isna(amount):
            return 0.0
        return round(float(amount) / crores, 2)
    except Exception:
        return 0.0

st.title("AI SQL + SPOC Name Pivot Integration")

user_input = st.text_input("Enter your question for the database:")

if user_input:
    with st.spinner("Generating SQL and fetching results..."):
        try:
            schema_info = get_full_schema_info()
            st.subheader("Database Schema Provided to Gemini")
            st.code(schema_info, language="plaintext")

            generated_sql = generate_sql_from_prompt(user_input, schema_info)
            st.subheader("Generated SQL Query")
            st.code(generated_sql, language="sql")

            result = query_db(generated_sql)

            if isinstance(result, pd.DataFrame):
                st.write("Raw columns:", result.columns.tolist())
                st.write(result.head())

                if all(col in result.columns for col in ['classification', 'nb', 'total_loan_amount']):
                    result = result.drop_duplicates(subset=['classification', 'nb', 'total_loan_amount'])

                    pivot_df = result.pivot_table(
                        index='classification',
                        columns='nb',
                        values='total_loan_amount',
                        aggfunc='sum',
                        fill_value=0
                    )
                    pivot_df = pivot_df.applymap(convert_to_crores)
                    pivot_df['TOTAL'] = pivot_df.sum(axis=1)
                    grand_total = pivot_df.sum(axis=0)
                    grand_total.name = 'Grand Total'
                    pivot_df = pd.concat([pivot_df, grand_total.to_frame().T])

                    st.subheader("Pivot Table with Totals (Values in Crores)")
                    st.dataframe(pivot_df.reset_index())
                else:
                    st.subheader("Query Results")
                    st.dataframe(result)

                # Manual SPOC targets in crores scale (adjust to your data scale)
                spoc_targets = {
                    'kavin': 0.75,
                    'kumar': 0.50,
                    # Add other SPOCs and their targets here
                }

                if 'spoc_name' in result.columns and 'total_loan_amount' in result.columns:
                    # Aggregate loan amounts per SPOC (Achievement)
                    achievement_df = result.groupby('spoc_name', as_index=False)['total_loan_amount'].sum()
                    achievement_df['loanamount'] = achievement_df['total_loan_amount'].apply(convert_to_crores)

                    target_df = pd.DataFrame(list(spoc_targets.items()), columns=['spoc_name', 'Target'])

                    # Merge achievements with targets
                    spoc_summary_df = achievement_df.merge(target_df, on='spoc_name', how='left')

                    spoc_summary_df['Target'] = spoc_summary_df['Target'].fillna(0)

                    # Rearrange columns and drop old column
                    spoc_summary_df = spoc_summary_df[['spoc_name', 'Target', 'loanamount']]

                    # Calculate and append Grand Total row
                    grand_total = pd.DataFrame({
                        'spoc_name': ['Grand Total'],
                        'Target': [spoc_summary_df['Target'].sum()],
                        'loanamount': [spoc_summary_df['loanamount'].sum()]
                    })

                    spoc_summary_df = pd.concat([spoc_summary_df, grand_total], ignore_index=True)

                    st.subheader("SPOC-wise Target and Loan Amount Summary")
                    st.dataframe(spoc_summary_df.style.format({
                        'Target': '{:.2f}',
                        'loanamount': '{:.2f}'
                    }))

                else:
                    st.info("SPOC summary not available: requires 'spoc_name' and 'total_loan_amount' columns in query result")

            else:
                st.error(result)

        except Exception as e:
            st.error(f"Error during SQL generation or execution: {e}")
