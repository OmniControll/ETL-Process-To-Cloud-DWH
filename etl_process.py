import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv
import logging

# Configure logging, this makes sure that the logs are written to the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from a .env
load_dotenv()

def extract():
    # 
    # EXTRACT PHASE
    # Extracts data to be loaded into the data warehouse or SQL server
    # This function can also connect to APIs or other data sources
    
    # 
    #creating sample data, but can also connect to different API's, CRMS etc.  in this block of code.

    data = pd.DataFrame({
        'ID': [1, 2, 3, 4, 5, 6, 7, 8],
        'Warehouse_block': ['D', 'F', 'A', 'B', 'C', 'F', 'D', 'F'],
        'Mode_of_Shipment': ['Flight'] * 8,
        'Customer_care_calls': [4, 4, 2, 3, 2, 3, 3, 4],
        'Customer_rating': [2, 5, 2, 3, 2, 1, 4, 1],
        'Cost_of_the_Product': [177, 216, 183, 176, 184, 162, 250, 233],
        'Prior_purchases': [3, 2, 4, 4, 3, 3, 3, 2],
        'Product_importance': ['low', 'low', 'low', 'medium', 'medium', 'medium', 'low', 'low'],
        'Gender': ['F', 'M', 'M', 'M', 'F', 'F', 'F', 'F'],
        'Discount_offered': [44, 59, 48, 10, 46, 12, 3, 48],
        'Weight_in_gms': [1233, 3088, 3374, 1177, 2484, 1417, 2371, 2804],
        'Reached_on_Time_Y_N': [1, 1, 1, 1, 1, 1, 1, 1]
    })
    logging.info("Data extracted successfully.")
    return data

#we're creating an  function here to validate the data we've received from the source.
#This function will check if the data has all the required columns and if the data types are correct.
def validate_data(data):
    """
    VALIDATE PHASE
    Validates the transformed data to ensure it meets the required criteria before loading
    """
    required_columns = ['ID', 'Warehouse_block', 'Mode_of_Shipment', 'Customer_care_calls', 
                        'Customer_rating', 'Cost_of_the_Product', 'Prior_purchases', 
                        'Product_importance', 'Gender', 'Discount_offered', 
                        'Weight_in_gms', 'Reached_on_Time_Y_N']
    
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Additional validations can be added here, such as data type checks
    logging.info("Data validation passed.")


#This function will clean the data by removing duplicates and handling missing values. Obviously, we can add more transformation steps here
#such as feature engineering, normalization, etc. based on the requirements of the project.
def transform(data):

    # """
    # TRANSFORM PHASE
    # Cleans the extracted data by removing duplicates and handling missing values
    # Performs necessary transformations on the data
    
    # """

    data = data.copy()  # To avoid SettingWithCopyWarning
    initial_count = len(data)
    data.drop_duplicates(inplace=True)
    duplicates_removed = initial_count - len(data)
    if duplicates_removed > 0:
        logging.info(f"Removed {duplicates_removed} duplicate rows.")
    data.fillna(method='ffill', inplace=True)
    logging.info("Data transformed successfully.")
    return data


#This function will load the transformed data into the data warehouse. In this case, we are using Snowflake as the target data warehouse.
#We first establish a connection to Snowflake using SQLAlchemy and then create a table in the database if it doesn't exist.
#Finally, we insert the transformed data into the table.

def load(data, table_name='train_table'):

    # """
    # LOAD PHASE
    # Inserts the transformed data into a Snowflake data warehouse.
    
    # """

    # Snowflake connection parameters
    user = os.getenv('SNOWFLAKE_USER')          # Snowflake username
    password = os.getenv('SNOWFLAKE_PASSWORD')  # Snowflake password

# Check if credentials are available, sometimes the credentials are not set in the environment variables

    if not user or not password:
        raise ValueError("Snowflake credentials not set in environment variables.") #ensures that the credentials are entered in the env file

    account = 'immwogi-ni84898'                  # Snowflake account identifier
    warehouse = 'COMPUTE_WH'                     # Snowflake warehouse
    database = 'SHIP_DATABASE'                   # Target database
    schema = 'PUBLIC'                            # Target schema
    role = 'SYSADMIN'                            # Role

    # the Snowflake SQLAlchemy connection URL, this is important because 
    # it allows us to connect to the Snowflake database using SQLAlchemy
    connection_url = (
        f'snowflake://{user}:{password}@{account}/'
        f'{database}/{schema}?warehouse={warehouse}&role={role}'
    )

    # Initialize metadata
    # it allows us to define the table schema within the snowflake database
    metadata = MetaData()

    try:
        # Create SQLAlchemy engine for Snowflake
        engine = create_engine(connection_url)

#  connection

        with engine.connect() as connection:
            # Create the database if it doesn't exist
            create_db_query = f"CREATE OR REPLACE DATABASE {database};"
            connection.execute(text(create_db_query))
            logging.info(f"Database '{database}' ensured in Snowflake.")

            # Switch to the newly created database
            use_db_query = f"USE DATABASE {database};"
            connection.execute(text(use_db_query))
            logging.info(f"Using database '{database}'.")

            # Define the table schema (if it doesn't exist)
            train_table = Table(
                table_name, metadata,
                Column('ID', Integer, primary_key=True, nullable=False),
                Column('Warehouse_block', String),
                Column('Mode_of_Shipment', String),
                Column('Customer_care_calls', Integer),
                Column('Customer_rating', Integer),
                Column('Cost_of_the_Product', Integer),
                Column('Prior_purchases', Integer),
                Column('Product_importance', String),
                Column('Gender', String),
                Column('Discount_offered', Integer),
                Column('Weight_in_gms', Integer),
                Column('Reached_on_Time_Y_N', Integer)
            )
            metadata.create_all(engine)
            logging.info(f"Table '{table_name}' ensured in Snowflake.")

            # Insert data into Snowflake table
            data.to_sql(
                name=table_name,
                con=connection,
                if_exists='append',  # Options: 'fail', 'replace', 'append'
                index=False,
                method='multi'       # Optimizes insertion by batching
            )
        
        logging.info(f"Data successfully loaded into Snowflake table '{table_name}'.")

    except SQLAlchemyError as e:
        # Handle SQLAlchemy-specific errors
        logging.error("An error occurred while loading data to Snowflake:")
        logging.error(e)
    except Exception as ex:
        # Handle other exceptions
        logging.error("An unexpected error occurred:")
        logging.error(ex)


#This is the main function that orchestrates the ETL process by calling the extract transform validate and load functions
#It also handles exceptions that may occur during the ETL process.

def main():
    # """
    # Main Function:
    # Orchestrates the ETL process
    # """

    try:
        # EXTRACT
        data = extract()

        # TRANSFORM
        transformed_data = transform(data)

        # VALIDATE
        validate_data(transformed_data)

        # LOAD
        load(transformed_data, table_name='train_table')

    except Exception as e:
        logging.critical("ETL process failed:")
        logging.critical(e)

if __name__ == "__main__":
    main()
