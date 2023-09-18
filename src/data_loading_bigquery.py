import logging
from google.cloud import bigquery
from .exceptions import ETLException
from .config import (
    SQLITE_TABLE,
    DEFAULT_PROJECT_ID,
    DEFAULT_DATASET_ID
)

# Function to create or update the table in BigQuery
def create_or_update_bigquery_table(client, schema, partitioning):
    """
    Create or update the BigQuery table with the specified schema and partitioning settings.

    Args:
        client (google.cloud.bigquery.client.Client): BigQuery client.
        schema (list of google.cloud.bigquery.schema.SchemaField): Table schema.
        partitioning (google.cloud.bigquery.table.TimePartitioning): Partitioning settings.
    """
    # Specify your GCP dataset ID
    dataset_id = DEFAULT_DATASET_ID
    # Specify the SQLite table
    table_id = SQLITE_TABLE

    # Initialize the BigQuery client

    # Check if the table exists
    table_ref = client.dataset(dataset_id).table(table_id)
    table_exists = False
    try:
        client.get_table(table_ref)
        table_exists = True
    except Exception as exc:
        table_exists = False
        error_message = f"An error occurred while getting the {table_ref} table from Bigquery: {str(exc)}"
        logging.error(error_message)
        raise ETLException(error_message)

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
    """
    Load data to Google BigQuery.

    Args:
        rows (list of dict): Transformed weather data as a list of dictionaries.
    """
    # Specify your GCP project and dataset ID
    project_id = DEFAULT_PROJECT_ID
    dataset_id = DEFAULT_DATASET_ID
    table_id = SQLITE_TABLE

    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

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
    create_or_update_bigquery_table(client, schema, partitioning)

    # Load data into BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)
    job = client.load_table_from_json(rows, table_ref)
    job.result()