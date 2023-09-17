import requests
import sqlite3
from datetime import datetime
import traceback

# Define the list of cities and their coordinates
cities = [
    {"name": "Medellin", "latitude": 6.2518, "longitude": -75.5636},
    {"name": "Bogota", "latitude": 4.6097, "longitude": -74.0817},
    {"name": "Cali", "latitude": 3.4372, "longitude": -76.5225}
]

# Function to extract weather data from the API for a given city
def extract_data(city):
    # Define the API URL for the city using its latitude and longitude
    latitude = city['latitude']
    longitude = city['longitude']
    api_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&forecast_days=10"

    try:
        # Make a GET request to the API
        response = requests.get(api_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            return data
        else:
            print(f"Failed to retrieve data for {city['name']}. Status Code: {response.status_code}")
            return None

    except Exception as e:
        print(f"An error occurred while extracting data for {city['name']}: {str(e)}")
        traceback.print_exc()
        return None


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
def create_or_replace_database():
    # Create SQLite database connection
    conn = sqlite3.connect("sqlite_database.db")
    cursor = conn.cursor()

    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS weather_data")

    # Create a table to store weather data
    cursor.execute('''CREATE TABLE weather_data (
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
    create_or_replace_database()

    # Create SQLite database connection
    conn = sqlite3.connect("sqlite_database.db")
    cursor = conn.cursor()

    # Insert transformed data into the database
    for city in data:
        cursor.execute('''INSERT INTO weather_data 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (city['city_name'], city['collection_timestamp'], city['measure_datetime'],
                        city['temperature_celsius'], city['temperature_fahrenheit'],
                        city['temperature_kelvin'], city['humidity'], city['wind_speed_m_s']))

    # Commit changes and close the database connection
    conn.commit()
    conn.close()

# Main function to orchestrate the ETL process
def main():
    extracted_data = []

    # Data Extraction (E)
    for city in cities:
        weather_data = extract_data(city)
        if weather_data:
            transformed_data = transform_data(weather_data, city['name'])
            if transformed_data:
                extracted_data.extend(transformed_data)

    # Data Loading (L)
    load_data(extracted_data)

if __name__ == "__main__":
    main()
