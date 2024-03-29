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

## Description

Airflow:

- Airflow is an orchestration tool used to automate process.
- In our application ETL pipeline is orchestrated via airflow, 3 tasks are created to achieve it.
-  process_recent_pdf_from_S3_task is created to process the latest pdf that was recently added based on timestamp.
-  extract_and_process_data_task is used to extract the text from the updated pdf file and return it in form of string, the string is then conveted in a dataframe after structuring validating and cleaning the string.
-  upload_to_database_task is used to uplad the cleaned dataframe into a snowflake database

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
https://codelabs-preview.appspot.com/?file_id=1X3w1C1zy9iA9h1K6L0akphRAeo6Wu1083fuXjX_sur4#0

  ## Contribution

| Contributor | Contributions            | Percentage |
|-------------|--------------------------|------------|
| Dev Mithunisvar Premraj       | Created Dag in Airflow, worked on the tasks() in airflow, created the streamlit UI.|33.33%|
| Aneesh Koka        | Worked on Dag in Airflow, GCP and deployment | 33.33% |
| Rishabh Shah         | Created two Fast API for user interaction and trigerring airflow, worked on DAG| 33.33% |

