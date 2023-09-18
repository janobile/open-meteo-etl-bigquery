import sqlite3
from src.config import SQLITE_TABLE, SQLITE_DATABASE_PATH


# Function to create or replace the SQLite database
def create_sqlite_database():
    """
    Create or replace the SQLite database and weather_data table.
    """
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
def load_data_sqlite(data):
    """
    Load transformed data into the SQLite database.

    Args:
        data (list of dict): Transformed weather data as a list of dictionaries.
    """
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