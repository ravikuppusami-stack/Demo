import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from google.genai import genai
from dotenv import load_dotenv
import os
import smtplib
from email.message import EmailMessage

# Load environment variables (for mail IDs and passwords as well)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Define these in your .env or secure environment
SENDER_EMAIL = os.getenv("SENDER_EMAIL")        # backend email
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")  # backend app password
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")  # backend recipient

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
ensuring that all table and column names are enclosed in backticks (`) to respect case sensitivity.
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
            return None
        return round(float(amount) / crores, 2)
    except Exception:
        return None

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
            # Remove any existing grand total rows
            if 'spoc_name' in result.columns:
                result = result[result['spoc_name'] != 'Grand Total']

            # Detect loan amount columns and convert to crores
            loan_amount_cols = [col for col in ['loanamount', 'loan_amount', 'total_loan_amount', 'TotalLoanAmount_Cr'] if col in result.columns]
            if loan_amount_cols:
                loan_col = loan_amount_cols[0]
                result['loanamount'] = result[loan_col].apply(convert_to_crores)
                if loan_col != 'loanamount':
                    result = result.drop(columns=[loan_col])

            # Warn if columns missing
            for col in ['Target', 'loanamount']:
                if col not in result.columns:
                    st.warning(f"Note: '{col}' column not present in query result.")

            # Group by spoc_name without summing 'Target' and sum loanamount
            if 'spoc_name' in result.columns:
                grouped_loan = result.groupby('spoc_name', as_index=False)['loanamount'].sum() if 'loanamount' in result.columns else pd.DataFrame(columns=['spoc_name', 'loanamount'])
                grouped_target = result.groupby('spoc_name', as_index=False)['Target'].first() if 'Target' in result.columns else pd.DataFrame(columns=['spoc_name', 'Target'])

                if not grouped_loan.empty and not grouped_target.empty:
                    grouped = pd.merge(grouped_target, grouped_loan, on='spoc_name', how='outer')
                elif not grouped_loan.empty:
                    grouped = grouped_loan
                else:
                    grouped = grouped_target

                # Append grand total row
                grand_total = {'spoc_name': 'Grand Total'}
                if 'Target' in grouped.columns:
                    grand_total['Target'] = grouped['Target'].sum()
                if 'loanamount' in grouped.columns:
                    grand_total['loanamount'] = grouped['loanamount'].sum()

                grouped = pd.concat([grouped, pd.DataFrame([grand_total])], ignore_index=True)
                result = grouped

            send_email_with_df_html(result)
            st.success("Query result has been sent via email.")
        else:
            st.error(result)
