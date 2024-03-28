import streamlit as st
import boto3

# Constants for S3
S3_BUCKET_NAME = "bigdatacasestudy4"

# Initialize session state for storing filenames and page navigation
if 'listfilename' not in st.session_state:
    st.session_state.listfilename = []
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'login'

# Create a session using environment variables or configured AWS credentials
session = boto3.Session(
    aws_access_key_id='AKIA4MTWHTESY6QTXXEJ',
    aws_secret_access_key='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'
)
s3 = session.client('s3')

def login_page():
    st.title('Login to File Upload Service')
    name = st.text_input('Name: ')
    password = st.text_input("Password: ", type="password")
    
    if st.button("Login"):
        if name == "User123" and password == "Hello123":
            st.session_state.authenticated = True
            st.session_state.current_page = 'upload'
        else:
            st.error("Login Failed. Please check your credentials.")

def upload_page():
    st.title('Welcome to the File Upload Service')
    file = st.file_uploader("Select a file")
    
    if file is not None:
        if file.name not in st.session_state.listfilename:
            st.session_state.listfilename.append(file.name)  # Add the filename to the session state list
            try:
                s3.upload_fileobj(file, S3_BUCKET_NAME, file.name)
                st.success("File has been uploaded to S3 bucket successfully.")
                st.write(file.name)
            except Exception as e:
                st.error(f"Failed to upload file: {e}")
        else:
            st.warning("This file has already been uploaded.")

# Page navigation
if st.session_state.current_page == 'login':
    login_page()
elif st.session_state.current_page == 'upload' and st.session_state.authenticated:
    upload_page()
else:
    st.session_state.current_page = 'login'
    st.error("You are not authenticated. Please log in.")


#http://localhost:8501/
