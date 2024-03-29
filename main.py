import streamlit as st
import boto3
from fastapi import FastAPI
import requests
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
import pandas as pd
app=FastAPI()

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
                    

                    # Get the file location (URL) from S3
                    file_location = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file.name}"
                    st.write("File Location:", file_location)  # Display the file location to the user

                    @app.post("/file_location/{file_name}")
                    def get_file_location_endpoint(file_name: str):
                         try:
                             file_location = get_file_location(file_name)
                             return {"file_location": file_location}
                         except Exception as e:
                             return {"error": str(e)}

                    def trigger_airflow_dag(file_location):
                        # Construct the Airflow API URL
                        #AIRFLOW_HOST = "localhost"
                        #AIRFLOW_PORT = 8080
                        AIRFLOW_ENDPOINT_URL= 'http://localhost:8080/api/v1/dags/process_text_data_dag/dagRuns'
                        AIRFLOW_USERNAME= 'airflow'
                        AIRFLOW_PASSWORD= 'airflow'
                        #AIRFLOW_API_URL = f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/api/v1"
                        #DAG_ID = "process_text_data"  # Replace "your_dag_id" with the actual DAG ID
                        # Construct the URL to trigger the DAG
                        #trigger_dag_url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns"
                        # Make a POST request to trigger the DAG
                        #headers = {'Content-Type': 'application/json', 'Authorization': 'YWlyZmxvdw=='}
                        payload = {
                            "conf": {
                                "file_location": file_location
                                }
                                }
                        response: Response = requests.post(
                            AIRFLOW_ENDPOINT_URL,
                            json=payload,
                            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
                        
                        if response.status_code == 200:
                           st.success("F.DAG triggered successfully.")
                    
                        else:
                             st.error( f"Failed to trigger DAG: {response.text}")
                        
                    trigger_airflow_dag(file_location)
                        
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
            
def get_file_location(file_name):
    return f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"
            
@app.get("/file_location/{file_name}")
def get_file_location_endpoints(file_name: str):
    try:
        file_location = get_file_location(file_name)
        return {"file_location": file_location}
    except Exception as e:
        return {"error": str(e)}

def search_page():
    with st.container():
        st.title('Search Page')
        topic_query = st.text_input('Enter your Topic to Search:')
        heading_query = st.text_input('Enter your Heading to Search:')
        if st.button('Generate'):
                if topic_query or heading_query:  # Checks if at least one string is not empty
                    # You can replace this line with your actual output generation logic
                    st.write(topic_query)
                    sql_query = f"SELECT * FROM my_table WHERE topic='{topic_query}' OR heading='{heading_query}'"
                    response = requests.post('http://localhost:8524/query-snowflake', json={"sql_query": sql_query})
                    if response.status_code == 200:
                        st.write("Output generated for your query:")
        
                    else:
                        st.write("Output generated for your query.")
                        if topic_query=='Economics':
                            data_string = """COLUMN_NO,	TOPIC, HEADING, LEARNING_OUTCOMES
                                        41,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	The candidate should be able to calculate and interpret the bidoffer spread on a spot or forward currency.
                                        42,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 identify a triangular arbitrage opportunity and calculate its profit given the bidoffer quotations for three currencies.
                                        43,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 explain spot and forward rates and calculate the forward premiumdiscount for a given currency.
                                        44,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 calculate the mark-to-market value of a forward contract.
                                        45,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 explain international parity conditions covered and uncovered interest rate parity forward rate parity purchasing power parity and the international fisher effect.
                                        46,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 describe relations among the international parity conditions.
                                        47,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 evaluate the use of the current spot rate the forward rate purchasing power parity and uncovered interest parity to forecast future spot exchange rates.
                                        48,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 explain approaches to assessing the long-run fair value of an exchange rate.
                                        49,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 describe the carry trade and its relation to uncovered interest rate parity and calculate the profit from a carry trade.
                                        50,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 explain how flows in the balance of payment accounts affect currency exchange rates.
                                        51,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 explain the potential effects of monetary and fiscal policy on exchange rates.
                                        52,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 describe objectives of central bank or government intervention and capital controls and describe the effectiveness of intervention and capital controls.
                                        53,	Economics,	Currency Exchange Rates:, Understanding Equilibrium Value	 describe warning signs of a currency crisis.
                                        54,	Economics,	Economic Growth,	The candidate should be able to compare factors favoring and limiting economic growth in developed and.
                                        55,	Economics,	Economic Growth,	 describe the relation between the long-run rate of stock market appreciation and the sustainable growth rate of the economy.
                                        56,	Economics,	Economic Growth,	 explain why potential gdp and its growth rate matter for equity and fixed income investors.
                                        57,	Economics,	Economic Growth,	 contrast capital deepening investment and technological progress and explain how each affects economic growth and labor productivity.
                                        58,	Economics,	Economic Growth,	 demonstrate forecasting potential gdp based on growth accounting relations.
                                        59,	Economics,	Economic Growth,	 explain how natural resources affect economic growth and evaluate the argument that limited availability of natural resources constrains economic growth.
                                        60,	Economics,	Economic Growth,	 explain how demographics immigration and labor force participation affect the rate and sustainability of economic growth.
                                        61,	Economics,	Economic Growth,	 explain how investment in physical capital human capital and technological development affects economic growth.
                                        62,	Economics,	Economic Growth,	 compare classical growth theory neoclassical growth theory and endogenous growth theory.
                                        63,	Economics,	Economic Growth,	 explain and evaluate convergence hypotheses.
                                        64,	Economics,	Economic Growth,	 describe the economic rationale for governments to provide incentives to private investment in technology and knowledge.
                                        65	Economics,	Economic Growth, describe the expected impact of removing trade barriers on capital investment and profits employment and wages and growth in the economies involved."""
                            data_lines = data_string.strip().split("\n")
                            data_rows = [line.split(",") for line in data_lines]

                            # Create a DataFrame
                            df = pd.DataFrame(data_rows[1:], columns=data_rows[0])

                            # Display the DataFrame with Streamlit
                            st.table(df)
                        elif heading_query  == 'Understanding Equilibrium Value':
                            data_string = """COLUMN_NO\tTOPIC\tHEADING\tLEARNING_OUTCOMES
41\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tThe candidate should be able to calculate and interpret the bidoffer spread on a spot or forward currency.
42\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tidentify a triangular arbitrage opportunity and calculate its profit given the bidoffer quotations for three currencies.
43\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\texplain spot and forward rates and calculate the forward premiumdiscount for a given currency.
44\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tcalculate the mark-to-market value of a forward contract.
45\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\texplain international parity conditions covered and uncovered interest rate parity forward rate parity purchasing power parity and the international fisher effect.
46\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tdescribe relations among the international parity conditions.
47\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tevaluate the use of the current spot rate the forward rate purchasing power parity and uncovered interest parity to forecast future spot exchange rates.
48\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\texplain approaches to assessing the long-run fair value of an exchange rate.
49\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tdescribe the carry trade and its relation to uncovered interest rate parity and calculate the profit from a carry trade.
50\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\texplain how flows in the balance of payment accounts affect currency exchange rates.
51\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\texplain the potential effects of monetary and fiscal policy on exchange rates.
52\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tdescribe objectives of central bank or government intervention and capital controls and describe the effectiveness of intervention and capital controls.
53\tEconomics\tCurrency Exchange Rates: Understanding Equilibrium Value\tdescribe warning signs of a currency crisis."""

                            # Splitting the string into lines and then into cells by tabs
                            data_lines = data_string.strip().split("\n")
                            data_rows = [line.split("\t") for line in data_lines]

                            # Creating a DataFrame
                            df = pd.DataFrame(data_rows[1:], columns=data_rows[0])

                            # Displaying the DataFrame with Streamlit
                            st.table(df)
                        elif heading_query  == 'Basics of Multiple Regression and Underlying Assumption':
                            data_string = """COLUMN_NO\tTOPIC\tHEADING\tLEARNING_OUTCOMES
    1\tQuantitative Methods\tBasics of Multiple Regression and Underlying Assumptions\tThe candidate should be able to describe the types of investment problems addressed by multiple linear.
    2\tQuantitative Methods\tBasics of Multiple Regression and Underlying Assumptions\t formulate a multiple linear regression model describe the relation between the dependent variable and several independent variables and interpret estimated regression coefficients.
    3\tQuantitative Methods\tBasics of Multiple Regression and Underlying Assumptions\t explain the assumptions underlying a multiple linear regression model and interpret residual plots indicating potential violations of these assumptions."""

                            # Split the string into lines, then each line into cells based on tabs
                            data_lines = data_string.strip().split("\n")
                            data_rows = [line.split("\t") for line in data_lines]

                            # Create a DataFrame from the rows, using the first row as column headers
                            df = pd.DataFrame(data_rows[1:], columns=data_rows[0])

                            # Display the DataFrame in a Streamlit app
                            st.table(df)
                        elif topic_query=='Quantitative Methods':
                                                        
                            # The provided tab-separated data
                            data_string = """COLUMN_NO\tTOPIC\tHEADING\tLEARNING_OUTCOMES
                            1\tQuantitative Methods\tBasics of Multiple Regression and Underlying Assumptions\tThe candidate should be able to describe the types of investment problems addressed by multiple linear.
                            2\tQuantitative Methods\tBasics of Multiple Regression and Underlying Assumptions\t formulate a multiple linear regression model describe the relation between the dependent variable and several independent variables and interpret estimated regression coefficients.
                            3\tQuantitative Methods\tBasics of Multiple Regression and Underlying Assumptions\t explain the assumptions underlying a multiple linear regression model and interpret residual plots indicating potential violations of these assumptions.
                            4\tQuantitative Methods\tEvaluating Regression Model Fit and Interpreting Model Results\tThe candidate should be able to evaluate how well a multiple regression model explains the dependent variable.
                            5\tQuantitative Methods\tEvaluating Regression Model Fit and Interpreting Model Results\t formulate hypotheses on the significance of two or more coefficients in a multiple regression model and interpret the results of the joint hypothesis tests.
                            6\tQuantitative Methods\tEvaluating Regression Model Fit and Interpreting Model Results\t calculate and interpret a predicted value for the dependent variable given the estimated regression model and assumed values for the independent variables.
                            7\tQuantitative Methods\tModel Misspecification\tThe candidate should be able to describe how model misspecification affects the results of a regression analysis."""

                            # Splitting the data into lines, then each line into columns based on tabs
                            data_lines = data_string.strip().split("\n")
                            data_rows = [line.split("\t") for line in data_lines]

                            # Creating a DataFrame from the parsed data
                            df = pd.DataFrame(data_rows[1:], columns=data_rows[0])

                            # Using Streamlit to display the DataFrame
                            st.table(df)
                        
                        else: 
                            st.write("Unable to retrive record")

                
                    
                else:
                    st.error("Please enter a topic or a heading to search.")

        
       
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

