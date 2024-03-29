# Assignment 4
Architecture Diagram
![Architecture Diagram](https://github.com/BigDataIA-Spring2024-Sec1-Team1/Assignment4/blob/main/architecture_diagram.png)

Airflow ETL Architecture
![Airflow ETL Architecture](https://github.com/BigDataIA-Spring2024-Sec1-Team1/Assignment4/blob/main/airflow_etl_architecture.png)
## Part 1

### Prerequisites

Before running the script, ensure you have the following installed:
- Airflow
- Streamlit
- PyPDF/GROBID
- Pytest and Pydantic for testing and validation
- FastAPI
- Snowflake
- AWS account


You can install PyDantic using pip:
```
pip install pydantic
```
You can install PyTest using pip:
```
pip install pytest
```
## Technologies Used

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=Snowflake&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)
![Amazon AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=ApacheAirflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=Docker&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)


## Description

Streamlit:

- It is used in our application as an User Interface, so users can login and upload file and as well as look into content.

Airflow:

- Airflow is an orchestration tool used to automate process.
- In our application ETL pipeline is orchestrated via airflow, 3 tasks are created to achieve it.
-  process_recent_pdf_from_S3_task is created to process the latest pdf that was recently added based on timestamp.
-  extract_and_process_data_task is used to extract the text from the updated pdf file and return it in form of string, the string is then conveted in a dataframe after structuring validating and cleaning the string.
-  upload_to_database_task is used to upload the cleaned dataframe into a snowflake database

Pydantic:

- Pydantic is a Python library for data validation and settings management.
- It validates input data against defined data models, ensuring adherence to specified rules and constraints.
- Pydantic can manage application settings by defining configuration schemas and validating settings at runtime.
- It automatically converts input data to specified data types, handling type casting and coercion transparently.
- The library supports serialization and deserialization of data models to and from various formats like JSON, YAML, etc.
- Pydantic seamlessly integrates with Python's type hinting system, allowing developers to specify data types using standard Python syntax.

PyTest: 

- Pytest is a testing framework for Python applications.
- It prioritizes simplicity and ease of use with an intuitive syntax.
- Fixture support enables the definition of reusable setup and teardown code for tests.
- Parameterized testing allows running the same test with different input values.
- Pytest offers a wide range of built-in assertions for verifying test outcomes.
- It seamlessly integrates with other testing tools and libraries.
- Pytest is popular for its versatility, flexibility, and extensive ecosystem.

URLClass: 

ContentPDFClass: Defines a class ContentClass for representing content data. 
It includes fields like  heading, topic, and learning_outcomes, 
with validation rules for ensuring that certain fields do not contain quote characters






## CodeLab - 
https://codelabs-preview.appspot.com/?file_id=1RZZROUo16zwfhLtBKI-qXTX5UDkoSCVcJggsNoXKmKI#0

  ## Contribution

| Contributor | Contributions            | Percentage |
|-------------|--------------------------|------------|
| Dev Mithunisvar Premraj       | Created Dag in Airflow, worked on the tasks(extract_and_process_data_task, upload_to_database_task) in airflow, created the streamlit UI.|33.33%|
| Aneesh Koka        | worked on the tasks(extract_and_process_data_task, upload_to_database_task) in airflow, GCP and deployment | 33.33% |
| Rishabh Shah         | Created two Fast API for user interaction and trigerring airflow, created the task process_recent_pdf_from_S3_task on DAG| 33.33% |

