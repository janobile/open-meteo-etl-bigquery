import logging
import requests
from .config import API_BASE_URL
from .exceptions import ETLException
from .exceptions import DataValidationException

# Function to extract weather data from the API for a given city
def extract_weather_data(city):
    """
    Extract weather data from the Open Meteo API for a specific city.

    Args:
        city (dict): A dictionary containing the city's name, latitude, and longitude.

    Returns:
        dict or None: The extracted weather data as a dictionary or None if extraction fails.
    """
    
    # Construct the API URL using the provided city coordinates
    latitude = city['latitude']
    longitude = city['longitude']
    api_url = f"{API_BASE_URL}/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&forecast_days=10"

    # Make an HTTP GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Validate the structure of the API response
        if 'hourly' not in data or 'hourly_units' not in data:
            raise DataValidationException("Invalid API response format")

        # Perform data type validation
        for key in ['temperature_2m', 'relativehumidity_2m', 'windspeed_10m']:
            if key not in data['hourly'] or not isinstance(data['hourly'][key], list):
                raise DataValidationException("Invalid data type in API response")
        
        success_message = f"Successfully extracted data for {city['name']}."
        logging.info(success_message)
        return data
    else:
        raise ETLException(f"Failed to retrieve data for {city['name']}. Status Code: {response.status_code}")
