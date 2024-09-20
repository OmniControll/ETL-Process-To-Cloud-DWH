The **Snowflake ETL Pipeline** is a Python-based application designed to streamline the process of extracting data, transforming it, and loading it into a Snowflake data warehouse. This project uses Pandas for data manipulation, SQLAlchemy for database interactions, and dotenv for secure management of environment variables.
It ensures a secure, efficient, and automated data integration workflow, making it easier to manage and analyze large datasets.

## Features

- **Extract:** Gathers data from predefined sources and structures it into a Pandas DataFrame.
- **Transform:** Cleans and transforms the extracted data by removing duplicates and handling missing values.
- **Load:** Inserts the transformed data into Snowflake, with dynamic creation of databases and tables if they don't exist.

- **Dynamic Database and Table Creation:** Automatically creates the target database and table schema in Snowflake.
- **Secure Credential Management:** Utilizes environment variables to manage sensitive Snowflake credentials securely.
- **Comprehensive Logging:** Implements logging to monitor the ETL process and capture errors effectively.
- **Error Handling:** Robust error handling to manage and log SQLAlchemy and general exceptions.

## Prerequisites

Before setting up the project, ensure you have the following installed:

- **Python 3.6+**: [Download Python](https://www.python.org/downloads/)
- **pip**: Python package installer (comes with Python)
- **Snowflake Account**: Set up a Snowflake account. [Sign Up for Snowflake](https://signup.snowflake.com/)
- **Git**: For version control. [Download Git](https://git-scm.com/downloads)
