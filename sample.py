import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from google import genai
from dotenv import load_dotenv
import os
import smtplib
from email.message import EmailMessage

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

if not DATABASE_URL or not GEMINI_API_KEY or not SENDER_EMAIL or not SENDER_PASSWORD or not RECIPIENT_EMAIL:
    st.error("One or more environment variables missing. Please check .env for email/DATABASE variables.")
    st.stop()

engine = create_engine(DATABASE_URL)
client = genai.Client(api_key=GEMINI_API_KEY)

def get_full_schema_info():
    schema = """
Tables:
- `borrowers`(`borrower_id` int, `name` varchar(100), `contact` varchar(20), `address` text)
- `institute`(`institute_id` int, `brand_name` text, `spoc_name` text, `region` text, `created_at` datetime)
- `loan`(`case_id` int, `borrower_id` int, `institute_id` int, `manager_id` int, `classification` text, `loan_amount` double, `login_date` varchar(50), `approval_date` varchar(50), `UTR_timestamp` varchar(50), `nb` int)
- `managers`(`manager_id` int, `name` varchar(100), `target` int, `achievement` int, `created_at` datetime)
- `target`(`spoc_name` text, `Target` double)
"""
    return schema

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

Write a valid MySQL SQL query ONLY that fulfills the user's request,
ensuring all relevant columns are selected with correct aliases and casing.
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

def send_email_with_df_html(dataframe):
    html_content = dataframe.to_html(index=False)
    msg = EmailMessage()
    msg['Subject'] = "Query Results"
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg.add_alternative(f"""
    <html>
        <body>
            <p>Hello,</p>
            <p>Here is the query result you requested:</p>
            {html_content}
            <p>Regards,<br>Your App</p>
        </body>
    </html>
    """, subtype='html')
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
        smtp.send_message(msg)

st.title("Data Analysis")

user_input = st.text_input("Enter your question for the database:")

if user_input and st.button("Send Query Result to Mail"):
    with st.spinner("Generating SQL and fetching results..."):
        schema_info = get_full_schema_info()
        generated_sql = generate_sql_from_prompt(user_input, schema_info)
        result = query_db(generated_sql)
        if isinstance(result, pd.DataFrame):
            result.columns = [col.lower() for col in result.columns]

            if 'classification' not in result.columns or 'nb' not in result.columns:
                st.error("Required columns 'classification' or 'nb' not found.")
            else:
                grouped = result.groupby('classification', as_index=False)['nb'].sum()
                grand_total = pd.DataFrame({'classification': ['Grand Total'], 'nb': [grouped['nb'].sum()]})
                final_df = pd.concat([grouped, grand_total], ignore_index=True)

                st.dataframe(final_df)
                send_email_with_df_html(final_df)
                st.success("Query result with grouped nb by classification sent via email.")
        else:
            st.error(result)
