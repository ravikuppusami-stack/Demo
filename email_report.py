import os
import pandas as pd
import smtplib
from sqlalchemy import create_engine, text
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configurations from environment or directly here
DATABASE_URL = os.getenv("DATABASE_URL")
EMAIL_HOST = "smtp.gmail.com"
EMAIL_PORT = 587
EMAIL_HOST_USER = os.getenv("EMAIL_USER", "your-email@gmail.com")
EMAIL_HOST_PASSWORD = os.getenv("EMAIL_PASS", "your-app-password")
EMAIL_FROM = EMAIL_HOST_USER
EMAIL_TO = ["ravikuppusami@propelld.com"]  # Update manager emails!

# Initialize the SQLAlchemy engine
engine = create_engine(DATABASE_URL)


def run_query():
    """
    Query the DB for spoc, target, and loan amount.
    Converts loan amount to crores and rounds to 2 decimals.
    Adds a grand total row at the end.
    """
    sql = """
    SELECT
        i.spoc_name AS spoc,
        t.Target AS target,
        SUM(l.loan_amount) AS loanamount
    FROM
        loan l
    JOIN
        institute i ON l.institute_id = i.institute_id
    JOIN
        target t ON i.spoc_name = t.spoc_name
    GROUP BY
        i.spoc_name, t.Target
    """
    df = pd.read_sql(text(sql), engine)
    df['loanamount'] = (df['loanamount'] / 1e7).round(2)
    
    # Calculate grand total row
    grand_total = pd.DataFrame({
        "spoc": ["Grand Total"],
        "target": [df['target'].sum().round(2)],
        "loanamount": [df['loanamount'].sum().round(2)]
    })

    # Append grand total row
    df = pd.concat([df, grand_total], ignore_index=True)
    return df


def send_email(df):
    """
    Send email with the dataframe contents embedded in the email body as HTML table.
    """
    subject = "Target Vs Achievement"

    # Convert dataframe to HTML table
    html_table = df.to_html(index=False)

    # Compose email body with HTML table
    body = f"""
    <html>
      <body>
        <p>Please find below the latest report:</p>
        {html_table}
      </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg['From'] = EMAIL_FROM
    msg['To'] = ", ".join(EMAIL_TO)
    msg['Subject'] = subject

    # Attach the HTML body to the email
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()

       

        server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())


def job():
    """
    Main scheduled job: run query and send report.
    """
    df = run_query()
    send_email(df)
    print("Email sent!")


if __name__ == "__main__":
    # For one-off manual test, uncomment below:
    job()

    # For scheduled daily send, e.g. 9:00 AM
    # scheduler = BlockingScheduler()
    # scheduler.add_job(job, 'cron', hour=9, minute=0)
    # print("Scheduler started... Waiting for scheduled jobs.")
    # scheduler.start()
