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



dag = DAG('process_text_data',
          description='Process text data containing educational syllabus',
          schedule_interval=None,
          start_date=datetime(2024, 1, 1),
          catchup=False)


    
# Define the PythonOperator to extract text from the PDF
def process_text_data():

        
    def process_recent_pdf_from_s3(bucket_name, aws_conn_id='aws_default'):
        """
        Process the most recently uploaded PDF file from S3.

        :param bucket_name: S3 bucket name as a string.
        :param aws_conn_id: Airflow AWS connection ID as a string (defaults to 'aws_default').
        :return: Extracted text from the most recent PDF as a string.
        """
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_client = s3_hook.get_conn()
        
        # List all objects in the bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        pdf_files = [obj for obj in objects.get('Contents', []) if obj['Key'].endswith('.pdf')]
        
        # Sort the PDF files by LastModified date
        pdf_files.sort(key=lambda x: x['LastModified'], reverse=True)
        
        if not pdf_files:
            return "No PDF files found in the bucket."
        
        # Get the most recent PDF file
        most_recent_pdf = pdf_files[0]['Key']
        
        # Process the most recent PDF file
        obj = s3_hook.get_key(most_recent_pdf, bucket_name=bucket_name)
        if obj:
            pdf_file = BytesIO(obj.get()['Body'].read())
            reader = PdfReader(pdf_file)
            extracted_text = ''
            
            for page in reader.pages:
                page_text = page.extract_text() + '\n' if page.extract_text() else ''
                extracted_text += page_text
            
            return extracted_text

        return "Failed to process PDF file."


    # Correct usage with required arguments
    bucket_name = 'bigdatacasestudy4'
    text  = process_recent_pdf_from_s3(bucket_name)







    def extract_titles_from_text(text):
        # Define regex to match sections that might precede "LEARNING OUTCOMES"
        pattern = re.compile(r'(?P<title>[A-Z][\w\s]+)\nLEARNING OUTCOMES', re.MULTILINE)

        titles = []

        for match in pattern.finditer(text):
            title = match.group("title").strip()
            # Split the title by new lines and spaces to check the conditions
            lines = title.split("\n")
            last_line_words = lines[-1].split()
            if len(last_line_words) == 1 and len(lines) > 1:  # If the last line has 1 word, check the line before it
                pre_last_line_words = lines[-2].split()
                if len(pre_last_line_words) >= 2 and all(word[0].isupper() for word in pre_last_line_words):
                    title = f"{lines[-2]} {last_line_words[0]}"
            titles.append(title)

        return titles


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
    df = pd.DataFrame(data_corrected, columns=["Column No", "Topic", "Heading", "Learning Outcomes"])

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
        df['Learning Outcomes'] = df['Learning Outcomes'].apply(clean_learning_outcome)
        df['Topic'] = df['Topic'].apply(clean_topics)
        return df

    processed_df = process_dataframe(df)
    print(processed_df.head())



process_text_task = PythonOperator(task_id='process_text_data_task',
                                   python_callable=process_text_data,
                                   dag=dag)

