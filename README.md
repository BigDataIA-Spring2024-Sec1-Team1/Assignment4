# Assignment 4
![Architecture Diagram]([https://github.com/BigDataIA-Spring2024-Sec1-Team1/Assignment4/blob/main/architecture_diagram.png])
## Part 1

### Prerequisites

Before running the script, ensure you have the following installed:
- Python
- Selenium
- Chrome WebDriver (or a driver for your preferred browser)
- Pytest and Pydantic for testing and validation


You can install PyDantic using pip:
```
pip install pydantic
```
You can install PyTest using pip:
```
pip install pytest
```

## Description

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
It includes fields like level, title, topic, and learning_outcomes, 
with validation rules for ensuring that certain fields do not contain HTML or quote characters

MetadataPDFClass: Defines a Pydantic model class, DocumentMetadata, which is used for documenting metadata of documents. 
It includes fields like file_size_bytes, num_pages, s3_grobid_text_link, file_path, encryption, and date_updated. 
The model also includes validators for ensuring that certain fields meet specific criteria, 
such as being positive integers, having a specific date format, and not containing HTML or quote characters.

## Part 2: Using DBT

### Prerequisites

Before running the script, ensure you have the following installed:
- DBT 
- Snowflake
  
## Description

DBT:

- DBT stands for Data Build Tool.
- It's an open-source command-line tool for transforming, testing, and documenting data in a data warehouse.
- DBT focuses on SQL-based transformations applied to data residing in a data warehouse.
- Transformations are organized into modular SQL files called "models."
- It automatically manages dependencies between models for efficient incremental builds.
- DBT includes a built-in testing framework for data quality checks and assertions.
- It generates documentation for data models based on metadata defined in SQL files.
- Projects are typically version-controlled using Git for tracking changes and collaboration.

  ## Steps

- Loaded the clean data into Snowflake
- You intend to create a summary table with the following schema using DBT. (Level, Topic, Year, Number of articles, Min Length (Summary), Max Length (Summary), Min Length (Learning outcomes), Max Length (Learning outcomes))
- Materialize it to a new table
- Write tests to validate the new columns
- Document your model
- Commit and Deploy the model
- Create a Test and Production Environment



## CodeLab - 
https://codelabs-preview.appspot.com/?file_id=1X3w1C1zy9iA9h1K6L0akphRAeo6Wu1083fuXjX_sur4#0

  ## Contribution

| Contributor | Contributions            | Percentage |
|-------------|--------------------------|------------|
| Dev Mithunisvar Premraj       | Web scraping data into CSV File and created 9 validations using Pydantic and 15 test cases( 6 pass and 9 fail) using Pytest, created URL class. Created architecture diagram. Cleaned the CSV file which was webscrapped and made a cleaned CSV file.    | 33.33% |
| Aneesh Koka        | Created two PDF classes, MetaDataPDFClass and ContentPDFClass, to represent the schema for the Grobid output. Created clean CSV files for the two classes. Performed data and schema validation using Pydantic 2. Built 5+5 test cases using Pytest for each of the classes to show how validation would succeed/fail (5 pass and 5 fail) | 33.33% |
| Rishabh Shah         | Loaded the clean CSV file into Snowflake, created a summary table using DBT, wrote tests to validate columns, documented model, committed and deployed the model using DBT| 33.33% |

