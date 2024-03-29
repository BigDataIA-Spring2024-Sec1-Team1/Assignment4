from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from PyPDF2 import PdfReader
import pandas as pd
import os
import re
import boto3
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from PyPDF2 import PdfReader
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import numpy as np
from airflow.hooks.base_hook import BaseHook
from sqlalchemy.exc import SQLAlchemyError



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 29),
    'retries': 1,
}

dag = DAG('process_text_data_dag',
          default_args=default_args,
          description='A DAG to process text data',
          schedule_interval=None)

def process_recent_pdf_from_s3(bucket_name, aws_conn_id='aws_default', **kwargs):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()
    objects = s3_client.list_objects_v2(Bucket=bucket_name)
    pdf_files = [obj for obj in objects.get('Contents', []) if obj['Key'].endswith('.pdf')]
    pdf_files.sort(key=lambda x: x['LastModified'], reverse=True)
    if not pdf_files:
        raise ValueError("No PDF files found in the bucket.")
    most_recent_pdf = pdf_files[0]['Key']
    obj = s3_hook.get_key(most_recent_pdf, bucket_name='bigdatacasestudy4')
    if obj:
        pdf_file = BytesIO(obj.get()['Body'].read())
        reader = PdfReader(pdf_file)
        extracted_text = ''
        for page in reader.pages:
            page_text = page.extract_text() + '\n' if page.extract_text() else ''
            extracted_text += page_text
        kwargs['ti'].xcom_push(key='extracted_text', value=extracted_text)

def extract_titles_from_text(text):
    pattern = re.compile(r'(?P<title>[A-Z][\w\s]+)\nLEARNING OUTCOMES', re.MULTILINE)
    titles = []
    for match in pattern.finditer(text):
        titles.append(match.group("title").strip())
    return titles

def extract_and_process_data(**kwargs):
    ti = kwargs['ti']
    text = ti.xcom_pull(key='extracted_text', task_ids='process_recent_pdf_from_s3_task')
    titles_before_outcomes = extract_titles_from_text(text)
   # Splitting the text into lines
    lines = text.split("\n")

        # Initialize the lists and variables for processing the document
    data = []
    current_topic = ""
    current_heading = ""
    outcome = ""

        # Using enumerate to keep track of the current index
    for i, line in enumerate(lines):
        line = line.strip()
        if not line or line == "LEARNING OUTCOMES":
            continue
        if line[0].isupper() and not line.startswith("□") and not "The candidate should be able to" in line:
                # Detect new topic or heading
                if current_topic and current_heading and outcome:
                    data.append([current_topic, current_heading, outcome.strip("□ ").replace("\n", " ")])
                    outcome = ""
                for val in titles_before_outcomes:

                    if val in line:  # Reset for new major topic
                        current_topic = line
                        current_heading = ""
                    else:
                        current_heading = line
        elif line.startswith("□") or "The candidate should be able to" in line:
                # Append directly if it's part of the outcomes
                if outcome:  # Add previous outcome
                    data.append([current_topic, current_heading, outcome.strip("□ ").replace("\n", " ")])
                    outcome = ""
                outcome = line
            # Catch-all for additional outcome lines, if any
        if i == len(lines) - 1 and outcome:  # Ensure the last outcome is added
                data.append([current_topic, current_heading, outcome])

        # Correcting initial list setup for DataFrame
    data_corrected = [[i+1, item[0], item[1], item[2]] for i, item in enumerate(data)]
    df = pd.DataFrame(data_corrected, columns=["Column_No", "Topic", "Heading", "Learning_Outcomes"])
    
    def clean_learning_outcome(val):
        val = val.replace('\t', ' ')
        cleaned_val = re.sub(r'[^\w\s.-]', '', val)
        if not cleaned_val.endswith('.'):
            cleaned_val += '.'
        cleaned_val = cleaned_val.capitalize()
        return cleaned_val

    def clean_topics(val):
        # Removing integers
        cleaned_val = re.sub(r'\d+', '', val)
        return cleaned_val

    def process_dataframe(df):
        # Apply cleaning functions to their respective columns
        df['Learning_Outcomes'] = df['Learning_Outcomes'].apply(clean_learning_outcome)
        df['Topic'] = df['Topic'].apply(clean_topics)
        return df

    df = process_dataframe(df)
    print(df.head(5))
    # Serialize and push the DataFrame for the next task
    ti.xcom_push(key='processed_data', value=df.to_json(orient='split'))

def upload_to_database(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='processed_data', task_ids='extract_and_process_data_task')
    df = pd.read_json(df_json, orient='split')
    df.columns = [c.upper() for c in df.columns]

    # Constants for Snowflake
    DB_NAME = 'Details'
    WAREHOUSE = 'cfa_dev_warehouse'
    TABLECONTENT = 'content_detail'
    TABLE_META = 'metadata_details'
    USERNAME = 'anee13'
    PASSWORD = 'AK@indinc101010'
    ACCOUNT = 'dpedynz-sr19718'

    # Configure connection URL for Snowflake
    connection_url = URL.create(
        "snowflake",
        username=USERNAME,
        password=PASSWORD,
        host=ACCOUNT,
        database=DB_NAME,
        query={
            'warehouse': WAREHOUSE,
            'role': 'ACCOUNTADMIN',  # Adjust role as necessary
        }
    )

    engine = create_engine(connection_url)

    try:
        with engine.connect() as conn:
            # Setup database and warehouse
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            conn.execute(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 180 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE")
            conn.execute(f"USE WAREHOUSE {WAREHOUSE}")
            conn.execute(f"USE DATABASE {DB_NAME}")

            # Setup tables
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {TABLECONTENT} (
                Column_No INT,
                Topic TEXT,
                Heading TEXT,
                Learning_Outcomes TEXT
            );""")

            conn.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE_META} (
                file_size INT,
                page_count INT,
                s3_text_link TEXT, 
                file_location TEXT,
                encryption_status TEXT,
                last_modified TEXT
            );""")

           
                    

            # (The rest of your database operation code goes here...)
            # For demonstration, loading DataFrame to 'content_detail' table
            df.to_sql(TABLECONTENT, con=conn, if_exists='append', index=False, schema='PUBLIC')

            print('Data upload successful.')

    except Exception as error:
        print(f"An error occurred: {error}")

    finally:
        engine.dispose()

with dag:
    process_pdf = PythonOperator(
        task_id='process_recent_pdf_from_s3_task',
        python_callable=process_recent_pdf_from_s3,
        op_kwargs={'bucket_name': 'bigdatacasestudy4'},
    )

    extract_process = PythonOperator(
        task_id='extract_and_process_data_task',
        python_callable=extract_and_process_data,
    )

    upload_db = PythonOperator(
        task_id='upload_to_database_task',
        python_callable=upload_to_database,
    )

    process_pdf >> extract_process >> upload_db
     
