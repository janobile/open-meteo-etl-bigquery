from datetime import datetime
from .exceptions import ETLException

def transform_weather_data(data, city_name):
    """
    Transform weather data obtained from the Open Meteo API.

    Args:
        data (dict): The raw weather data as a dictionary.
        city_name (str): The name of the city for which data is being transformed.

    Returns:
        list of dict or None: The transformed weather data as a list of dictionaries, or None if transformation fails.
    """
    try:
        if data is None:
            return None

        transformed_data = []

        # Extract the required information
        hourly_data = data.get('hourly', {})

        # Get the current timestamp
        current_timestamp = datetime.utcnow()

        for i in range(len(hourly_data.get('time', []))):
            temperature_celsius = hourly_data['temperature_2m'][i]
            temperature_fahrenheit = (temperature_celsius * (9/5)) + 32
            temperature_kelvin = temperature_celsius + 273.15
            humidity = hourly_data['relativehumidity_2m'][i]
            wind_speed = hourly_data['windspeed_10m'][i] * (1000 / 3600)
            measure_datetime = datetime.strptime(hourly_data['time'][i], "%Y-%m-%dT%H:%M")

            # Create a dictionary for the transformed data
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
    except Exception as exc:
        raise ETLException(f"An error occurred while transforming data for {city_name}: {str(exc)}")