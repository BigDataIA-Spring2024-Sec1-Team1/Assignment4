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

# Custom CSS for changing background and text color
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def apply_custom_css():
    st.markdown("""
        <style>
        /* Apply a fading (gradient) background from one color to another */
        .stApp {
            background-image: linear-gradient(to right, #4F8BF9, #A4C0F9); /* Adjust the colors to change the gradient */
        }
        
        /* Change the text color for titles and text to black for better visibility */
        h1, h2, h3, h4, h5, h6, p, .stTextInput>div>div>input, .st-bb {
            color: #000000; /* Black text */
        }
        
        /* Style the button with black border and text */
        .stButton>button {
            border: 2px solid #000000; /* Black border */
            border-radius: 20px;
            color: #000000; /* Black text */
            background-color: #FFFFFF; /* White background */
            transition: all 0.3s ease; /* Smooth transition for hover effects */
        }

        /* Change button hover style */
        .stButton>button:hover {
            background-color: #f2f2f2; /* Lighter shade for hover background */
            color: #4F8BF9; /* Change text color on hover */
        }
        
        /* Style the file uploader with a black border */
        .stFileUploader {
            border: 2px dashed #000000; /* Black dashed border */
            border-radius: 20px;
            color: #000000; /* Black text */
            transition: border-color 0.3s ease; /* Smooth transition for hover effect */
        }

        /* Change file uploader hover style */
        .stFileUploader:hover {
            border-color: #4F8BF9; /* Change border color on hover */
        }
        </style>
    """, unsafe_allow_html=True)


def login_page():
    with st.container():
        st.title('Login to File Upload Service')
        
        col1, col2 = st.columns([1, 2])
        with col1:
            name = st.text_input('Name: ')
            password = st.text_input("Password: ", type="password")
            
            if st.button("Login"):
                if name == "User123" and password == "Hello123":
                    st.session_state.authenticated = True
                    st.session_state.current_page = 'upload'
                else:
                    st.error("Login Failed. Please check your credentials.")

def upload_page():
    with st.container():
        st.title('Welcome to the File Upload Service')
        file = st.file_uploader("Select a file")
        
        if file is not None:
            if file.name not in st.session_state.listfilename:
                st.session_state.listfilename.append(file.name)  # Add the filename to the session state list
                try:
                    s3.upload_fileobj(file, S3_BUCKET_NAME, file.name)
                    st.success("File has been uploaded to S3 bucket successfully.")
                except Exception as e:
                    st.error(f"Failed to upload file: {e}")
            else:
                st.warning("This file has already been uploaded.")
        if st.button('Logout'):
            st.session_state.authenticated = False
            st.session_state.current_page = 'login'
            st.experimental_rerun()
        if st.button('Search'):
            st.session_state.current_page = 'search'  # Update to lower case 'search'
            st.experimental_rerun()
        

# ... [rest of your imports and constants] ...

def search_page():
    with st.container():
        st.title('Search Page')
        search_query = st.text_input('Enter your search term:')
        
       
        if st.button('Logout from Search Page'):
            st.session_state.authenticated = False
            st.session_state.current_page = 'login'
            st.experimental_rerun()

def main():
    apply_custom_css()

    # Modify the 'elif' for the search page to match the updated session state value
    if st.session_state.current_page == 'login':
        login_page()
    elif st.session_state.current_page == 'upload' and st.session_state.authenticated:
        upload_page()
    elif st.session_state.current_page == 'search' and st.session_state.authenticated:  # This line was corrected
        search_page()
    else:
        st.session_state.current_page = 'login'
        st.error("You are not authenticated. Please log in.")

if __name__ == "__main__":
    main()
