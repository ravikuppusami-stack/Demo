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

if not all([DATABASE_URL, GEMINI_API_KEY, SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL]):
    st.error("Missing environment variables.")
    st.stop()

engine = create_engine(DATABASE_URL)
client = genai.Client(api_key=GEMINI_API_KEY)


def get_full_schema_info():
    return """Tables:
- `borrowers`(`borrower_id` int, `name` varchar(100), `contact` varchar(20), `address` text)
- `institute`(`institute_id` int, `brand_name` text, `spoc_name` text, `region` text, `created_at` datetime)
- `loan`(`case_id` int, `borrower_id` int, `institute_id` int, `manager_id` int, `classification` text, `loan_amount` double, `login_date` varchar(50), `approval_date` varchar(50), `UTR_timestamp` varchar(50), `nb` varchar(10))
- `managers`(`manager_id` int, `name` varchar(100), `target` int, `achievement` int, `created_at` datetime)
- `target`(`spoc_name` text, `Target` double)
"""


def clean_sql_query(sql):
    sql = sql.strip()
    if sql.startswith("```"):
        parts = sql.split("```")
        if len(parts) > 1:
            sql = parts[1]
    if sql.lower().startswith("sql"):
        sql = sql[3:].lstrip(" \n")
    return sql


def generate_sql_from_prompt(prompt, schema):
    full_prompt = f"""
You are an expert SQL assistant for a MySQL database with this schema:
{schema}

User request: "{prompt}"

Write a valid MySQL SQL query ONLY that fulfills the user's request,
ensuring all table and column names are enclosed in backticks (`).
"""
    response = client.models.generate_content(model="gemini-1.5-flash", contents=full_prompt)
    return response.text.strip()


def query_db(sql):
    sql = clean_sql_query(sql)
    try:
        return pd.read_sql(text(sql), engine)
    except Exception as e:
        return f"SQL Execution Error: {e}"


def convert_to_crores(amount):
    CRORES = 10000000
    try:
        if pd.isna(amount):
            return None
        return round(float(amount) / CRORES, 2)
    except:
        return None


def send_email_with_pivot_and_totals(df):
    required_cols = {'classification', 'nb', 'loan_amount'}
    if required_cols.issubset(df.columns):
        pivot_df = pd.pivot_table(
            df,
            index='classification',
            columns='nb',
            values='loan_amount',
            aggfunc='sum',
            fill_value=0
        )
        for col in ['ED', 'AB', 'AV', 'KSF']:
            if col not in pivot_df.columns:
                pivot_df[col] = 0.0
        pivot_df = pivot_df[['ED', 'AB', 'AV', 'KSF']]
        pivot_df['TOTAL'] = pivot_df.sum(axis=1)
        grand_total = pivot_df.sum(axis=0)
        grand_total.name = 'Grand Total'
        pivot_df = pd.concat([pivot_df, pd.DataFrame([grand_total])])
        out_df = pivot_df.reset_index()
    else:
        out_df = df

    html_content = out_df.to_html(index=False)
    msg = EmailMessage()
    msg['Subject'] = "Classification-wise Report"
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg.add_alternative(f"""
<html>
  <body>
    <p>Hello,</p>
    <p>Here is your requested query result:</p>
    {html_content}
    <p>Regards,<br>Your App</p>
  </body>
</html>
""", subtype='html')

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
        smtp.send_message(msg)


st.title("Disbursement")

user_input = st.text_input("Enter your question for the database:")

if user_input and st.button("Send Query Result to Mail"):
    with st.spinner("Generating and fetching results..."):
        schema = get_full_schema_info()
        sql = generate_sql_from_prompt(user_input, schema)
        result = query_db(sql)

        if isinstance(result, pd.DataFrame):
            if 'loan_amount' in result.columns:
                result['loan_amount'] = result['loan_amount'].apply(convert_to_crores)

            # Show the raw table (no pivot) in Streamlit app
            st.dataframe(result)

            # Send pivoted table only in email
            send_email_with_pivot_and_totals(result)

            st.success("Query result sent by email.")
        else:
            st.error(result)
