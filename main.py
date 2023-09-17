from datetime import datetime
import traceback
import requests
import logging
import sqlite3
from google.cloud import bigquery
from config import (
    API_BASE_URL,
    SQLITE_TABLE,
    DEFAULT_PROJECT_ID,
    DEFAULT_DATASET_ID,
    SQLITE_DATABASE_PATH,
    LOG_LEVEL,
)

# Configure logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=LOG_LEVEL, format=log_format)

# Define the list of cities and their coordinates
cities = [
    {"name": "Medellin", "latitude": 6.2518, "longitude": -75.5636},
    {"name": "Bogota", "latitude": 4.6097, "longitude": -74.0817},
    {"name": "Cali", "latitude": 3.4372, "longitude": -76.5225}
]

class ETLException(Exception):
    pass

# Function to extract weather data from the API for a given city
def extract_data(city):
    # Define the API URL for the city using its latitude and longitude
    latitude = city['latitude']
    longitude = city['longitude']
    api_url = f"{API_BASE_URL}/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&forecast_days=10"

    try:
        # Make a GET request to the API
        response = requests.get(api_url)

        # Check if the request was successful (status code 200)
        if response.status_code != 200:
            error_message = f"Failed to retrieve data for {city['name']}. Status Code: {response.status_code}"
            logging.error(error_message)
            raise ETLException(error_message)

        data = response.json()
        
        success_message = f"Successfully extracted data for {city['name']}."
        logging.info(success_message)

        return data

    except Exception as e:
        error_message = f"An error occurred while extracting data for {city['name']}: {str(e)}"
        logging.error(error_message)
        traceback.print_exc()
        raise ETLException(error_message)


# Function to transform data
def transform_data(data, city_name):
    if data is None:
        return None

    transformed_data = []

    # Extract the required information
    hourly_data = data['hourly']
    hourly_units = data['hourly_units']

    # Get the current timestamp
    current_timestamp = datetime.utcnow()

    # Iterate through the hourly data and extract information for each hour
    for i in range(len(hourly_data['time'])):
        temperature_celsius = hourly_data['temperature_2m'][i]
        temperature_fahrenheit = (temperature_celsius * (9/5)) + 32
        temperature_kelvin = temperature_celsius + 273.15
        humidity = hourly_data['relativehumidity_2m'][i]
        wind_speed = hourly_data['windspeed_10m'][i] * (1000 / 3600)
        measure_datetime = datetime.strptime(hourly_data['time'][i], "%Y-%m-%dT%H:%M")

        # Create a dictionary for the data of each hour
        hour_data = {
            'city_name': city_name,
            'collection_timestamp': current_timestamp,
            'measure_datetime': measure_datetime,
            'temperature_celsius': round(temperature_celsius, 2),
            'temperature_fahrenheit': round(temperature_fahrenheit, 2),
            'temperature_kelvin': round(temperature_kelvin, 2),
            'humidity': round(humidity, 2),
            'wind_speed_m_s': round(wind_speed, 2)
        }
        transformed_data.append(hour_data)

    return transformed_data

# Function to create or replace the SQLite database
def create_sqlite_database():
    # Create SQLite database connection
    conn = sqlite3.connect("sqlite_database.db")
    cursor = conn.cursor()

    # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {SQLITE_TABLE}")

    # Create a table to store weather data
    cursor.execute(f'''CREATE TABLE {SQLITE_TABLE} (
                        city_name TEXT,
                        collection_timestamp TIMESTAMP,
                        measure_datetime TIMESTAMP,
                        temperature_celsius REAL,
                        temperature_fahrenheit REAL,
                        temperature_kelvin REAL,
                        humidity REAL,
                        wind_speed_m_s REAL
                    )''')

    # Commit changes and close the database connection
    conn.commit()
    conn.close()

# Function to load data into an SQLite database
def load_data(data):
    if data is None:
        return

    # Create or replace the SQLite database
    create_sqlite_database()

    # Create SQLite database connection
    conn = sqlite3.connect(SQLITE_DATABASE_PATH)
    cursor = conn.cursor()

    # Insert transformed data into the database
    for city in data:
        cursor.execute(f'''INSERT INTO {SQLITE_TABLE} 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (city['city_name'], city['collection_timestamp'], city['measure_datetime'],
                        city['temperature_celsius'], city['temperature_fahrenheit'],
                        city['temperature_kelvin'], city['humidity'], city['wind_speed_m_s']))

    # Commit changes and close the database connection
    conn.commit()
    conn.close()

# Function to export data from SQLite and return the rows
def export_data_from_sqlite():

    # Export data from SQLite to BigQuery
    conn = sqlite3.connect(SQLITE_DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {SQLITE_TABLE}")
    rows = cursor.fetchall()
    conn.close()
    # Define a list to store the data in dictionaries
    data_dicts = []

    # Get the column names from the cursor description
    column_names = [description[0] for description in cursor.description]

    # Iterate through the rows and create dictionaries
    for row in rows:
        data_dict = {}
        for i, column_name in enumerate(column_names):
            data_dict[column_name] = row[i]
        data_dicts.append(data_dict)

    return data_dicts

# Function to create or update the table in BigQuery
def create_or_update_bigquery_table(schema, partitioning):
    # Specify your GCP project and dataset ID
    project_id = DEFAULT_PROJECT_ID
    dataset_id = DEFAULT_DATASET_ID
    table_id = SQLITE_TABLE

    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Check if the table exists
    table_ref = client.dataset(dataset_id).table(table_id)
    table_exists = False
    try:
        client.get_table(table_ref)
        table_exists = True
    except:
        table_exists = False

    # Create or update the table
    if table_exists:
        # If the table exists, update its schema and partitioning settings
        table = client.get_table(table_ref)
        table.schema = schema
        table.time_partitioning = partitioning
        client.update_table(table, ["schema", "time_partitioning"])
    else:
        # If the table does not exist, create it with the specified schema and partitioning
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = partitioning 
        client.create_table(table)

# Function to load data into BigQuery
def load_data_bigquery(rows):
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("city_name", "STRING"),
        bigquery.SchemaField("collection_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("measure_datetime", "TIMESTAMP"),
        bigquery.SchemaField("temperature_celsius", "FLOAT"),
        bigquery.SchemaField("temperature_fahrenheit", "FLOAT"),
        bigquery.SchemaField("temperature_kelvin", "FLOAT"),
        bigquery.SchemaField("humidity", "FLOAT"),
        bigquery.SchemaField("wind_speed_m_s", "FLOAT"),
    ]

    # Define the partitioning field
    partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,field="collection_timestamp")

    # Create or update the table in BigQuery
    create_or_update_bigquery_table(schema, partitioning)

    # Specify your GCP project and dataset ID
    project_id = DEFAULT_PROJECT_ID
    dataset_id = DEFAULT_DATASET_ID
    table_id = SQLITE_TABLE

    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Load data into BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)
    # print(len(rows))
    # print(type(rows[0]))
    # print(rows[0])
    # print(rows[0:10])
    # print(rows[-10:-1])
    job = client.load_table_from_json(rows, table_ref)
    job.result()

# Main function to orchestrate the ETL process
def main():
    try:
        extracted_data = []

        # Log the start of the ETL process
        logging.info("ETL process started.")

        # Data Extraction (E)
        for city in cities:
            try:
                weather_data = extract_data(city)
                if weather_data:
                    transformed_data = transform_data(weather_data, city['name'])
                    if transformed_data:
                        extracted_data.extend(transformed_data)
            except ETLException as e:
                logging.error(f"ETL error for {city['name']}: {str(e)}")

        # Data Loading (L)
        try:
            load_data(extracted_data)
            success_message = "Data loaded successfully into SQLite."
            logging.info(success_message)
        except ETLException as e:
            logging.error(f"ETL error: {str(e)}")

        # Export data from SQLite and return the rows
        try:
            rows = export_data_from_sqlite()
            success_message = "Data exported successfully from SQLite."
            logging.info(success_message)
        except ETLException as e:
            logging.error(f"ETL error: {str(e)}")

        # Load data into BigQuery
        try:
            load_data_bigquery(rows)
            success_message = "Data loaded successfully into BigQuery."
            logging.info(success_message)
        except ETLException as e:
            logging.error(f"ETL error: {str(e)}")

    except Exception as e:
        error_message = f"An unexpected error occurred: {str(e)}"
        logging.error(error_message)

    finally:
        # Log the end of the ETL process with timestamp
        logging.info("ETL process completed.")

if __name__ == "__main__":    
    main()
