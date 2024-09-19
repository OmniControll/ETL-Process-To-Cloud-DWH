import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

def extract():
    #EXTRACT PHASE
    # this is the data we want to load into the data warehouse or sql server, we can use this function also to connect to API's or other data sources.

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
    return data

def transform(data):

    # TRANSFORM PHASE
    # Clean the extracted data by removing duplicates and handling missing values. we perform some transformations on the data here

    data.drop_duplicates(inplace=True)
    data.fillna(method='ffill', inplace=True)
    return data

def load(data):
    #LOAD PHASE
    #Insert the transformed data into a SQLite database. # we can also specify to connect to a data warehouse here, or on premise server

    # Connection parameters
    db_path = 'train_data.db'  #  database file
    table_name = 'train_table'  # Target table name

    # create an engine to conn
    engine = create_engine(f'sqlite:///{db_path}')


    # Define metadata
    metadata = MetaData()

    # Define  schema
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
        Column('Reached_on_Time_Y_N', Integer)  # Renamed to avoid dots
    )

    # Create table if it doesn't exist
    metadata.create_all(engine)

    # Load data into  table
    data.to_sql(table_name, engine, if_exists='replace', index=False)

def main():
       # Main Function:
        #Orchestrates the ETL process by calling extract, transform, and load functions sequentially.

    #EXTRACT
    data = extract()

    #TRANSFORM
    transformed_data = transform(data)

    #LOAD
    load(transformed_data)

if __name__ == "__main__":
    main()
