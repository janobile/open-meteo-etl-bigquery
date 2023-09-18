import logging
import traceback

from src.config import LOG_LEVEL
from src.exceptions import ETLException
from src.exceptions import DataValidationException
from src.data_extraction_api import extract_weather_data 
from src.data_transformation import transform_weather_data
from src.data_loading_sqlite import load_data_sqlite
from src.data_loading_bigquery import load_data_bigquery
from src.data_extraction_sqlite import export_data_from_sqlite

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of cities and their coordinates
cities = [
    {"name": "Medellin", "latitude": 6.2518, "longitude": -75.5636},
    {"name": "Bogota", "latitude": 4.6097, "longitude": -74.0817},
    {"name": "Cali", "latitude": 3.4372, "longitude": -76.5225}
]

# Main function to orchestrate the ETL process
def main():
    try:
        extracted_data = []

        # Log the start of the ETL process
        logging.info("ETL process started.")

        # Data Extraction (E)
        for city in cities:
            try:
                weather_data = extract_weather_data(city)
                if weather_data:
                    transformed_data = transform_weather_data(weather_data, city['name'])
                    if transformed_data:
                        extracted_data.extend(transformed_data)
            except DataValidationException as exc:
                logging.error(f"Data validation error for {city['name']}: {str(exc)}")
                traceback.print_exc()
            except ETLException as exc:
                logging.error(f"ETL error for {city['name']}: {str(exc)}")
                traceback.print_exc()

        # Data Loading (L)
        try:
            load_data_sqlite(extracted_data)
            success_message = "Data loaded successfully into SQLite."
            logging.info(success_message)
        except ETLException as exc:
            logging.error(f"ETL error: {str(exc)}")
            traceback.print_exc()

        # Export data from SQLite and return the rows
        try:
            rows = export_data_from_sqlite()
            success_message = "Data exported successfully from SQLite."
            logging.info(success_message)
        except ETLException as exc:
            logging.error(f"ETL error: {str(exc)}")
            traceback.print_exc()

        # Load data into BigQuery
        try:
            load_data_bigquery(rows)
            success_message = "Data loaded successfully into BigQuery."
            logging.info(success_message)
        except ETLException as exc:
            logging.error(f"ETL error: {str(exc)}")
            traceback.print_exc()

    except Exception as exc:
        error_message = f"An unexpected error occurred: {str(exc)}"
        logging.error(error_message)
        traceback.print_exc()

    finally:
        # Log the end of the ETL process with timestamp
        logging.info("ETL process completed.")

if __name__ == "__main__":    
    main()
