# Open Meteo ETL with BigQuery Integration

This repository contains a Python-based ETL (Extract, Transform, Load) process that extracts weather data from the Open Meteo API, transforms the data, and loads it into a local SQLite database. It also includes a query for the data on Bigquery for further analysis.

## Project Structure
```bash
├── open-meteo-etl-bigquery/            # Main project directory.
│   ├── __init__.py                     
│   ├── queries/                        # Directory for SQL queries.
│   │   └── query.sql                   # SQL query to calculate the mean, minimum, and maximum temperatures per day per city for the next 10 days.
│   ├── src/                            # Source code directory.
│   │   ├── __init__.py 
│   │   ├── data_extraction_api.py      # Python script to extract data from the Open Meteo API.
│   │   ├── data_extraction_sqlite.py   # Python script to export data from SQLite.
│   │   ├── data_loading_bigquery.py    # Python script to load data into Google BigQuery.
│   │   ├── data_loading_sqlite.py      # Python script to load data into SQLite.
│   │   ├── data_transformation.py      # Python script for data transformation.
│   │   ├── exceptions.py               # Custom exception classes.
│   │   ├── config.py                   # Configuration settings.
│   │   └── main.py                     # Main script orchestrating the ETL process.
│   ├── .env                            # Environment-specific configuration file.
│   ├── credentials.json                # Service account credentials for Google Cloud services.
│   ├── LICENSE                         # MIT License file.
│   ├── requirements.txt                # List of Python dependencies.
└── README.md                           # Project README file.
```

## Features

### 1. Extract from API
- The ETL process starts by extracting weather data from the Open Meteo API for specified cities (Cali, Medellin, Bogota).
- Python script `data_extraction_api.py` fetches data from the API for each city.

### 2. Transform Data
- The raw data obtained from the API is transformed into a structured format.
- Temperature units are converted, values are rounded, and timestamps are added.
- Transformation logic is implemented in `data_transformation.py`.

### 3. Load Data into SQLite
- Transformed weather data is stored in a local SQLite database.
- The database schema and table creation are managed by `data_loading_sqlite.py`.

### 4. Extract Data from SQLite
- Transformed data can be exported from SQLite.
- The script `data_extraction_sqlite.py` exports data in a format suitable for loading into Google BigQuery.

### 5. Load Data in BigQuery
- Transformed data is loaded into Google BigQuery, a cloud-based data warehouse.
- The script `data_loading_bigquery.py` handles the process of creating or updating BigQuery tables and loading data.

### 6. SQL Query for BigQuery
- The project includes a SQL query (`queries/query.sql`) specifically designed for BigQuery.
- This query calculates the mean, minimum, and maximum temperatures per day per city for the next 10 days.

## Tech Stack

The following technologies and libraries were used in this project:

- Python 3.x
- SQLite
- Google BigQuery
- Google Cloud Platform (GCP)
- `requests` library for API requests
- `google-cloud-bigquery` library for interacting with BigQuery
- `dotenv` for managing environment variables

## Prerequisites

Before you start using this project, ensure that you have the following prerequisites in place:

- Google Cloud Platform (GCP) Account: You must have a GCP account to access BigQuery.
- Service Account Credentials: Obtain service account credentials in JSON format for your GCP project. These credentials are required for authentication and authorization when working with BigQuery. Make sure to grant appropriate permissions to the service account, including BigQuery access.
- Python (3.x recommended)
- Required Python libraries (see `requirements.txt`)

## Getting Started
To run this ETL process on your local machine, follow these steps:
1. Clone this repository to your local machine:

   ```shell
   git clone https://github.com/janobile/open-meteo-etl-bigquery.git
   cd open-meteo-etl-bigquery
    ```
2. Create and activate a virtual environment (optional but recommended):
    ```shell
    python -m venv venv
    source venv/bin/activate # On Windows, use: venv\Scripts\activate
    ```
3. Install the required Python dependencies:
    ```shell
    pip install -r requirements.txt
    ```

4. Set up your environment-specific configuration in the .env file:

- **VIRTUAL_ENV**: Name of your virtual environment (e.g., "venv").
- **GOOGLE_APPLICATION_CREDENTIALS**: Path to your Google Cloud service account credentials JSON file.
- **GCP_PROJECT_ID**: Your Google Cloud project ID.
- **GCP_DATASET_ID**: ID of the dataset in BigQuery.
- **LOG_LEVEL**: Logging level (default: INFO).

    ```shell
    # Environment-specific configuration
    VIRTUAL_ENV=venv

    # Google Cloud settings
    GOOGLE_APPLICATION_CREDENTIALS="./credentials.json"
    GCP_PROJECT_ID="your-project-id"
    GCP_DATASET_ID="your-dataset"

    # Logging settings
    LOG_LEVEL=INFO
    ```
5. Ensure you have SQLite installed on your system or update the SQLITE_DATABASE_PATH in `config.py` to point to the desired SQLite database location.

    ```
    pip install -r requirements.txt
    ```
6. Run the ETL process:
    ```
    python main.py
    ```

## Data Engineering Concepts
In this project, I applied the following data engineering concepts:

**Data Extraction Method Used**: I used Python's requests library to fetch data from the Open Meteo API. The API provides weather data in JSON format.

**Data Transformation Techniques Applied**: I transformed the raw data by extracting relevant information, converting temperature units, rounding values, and adding timestamps. The transformation is performed in the transform_weather_data function.

**Data Persistence Strategy Employed**: Transformed data is stored in a local SQLite database. The database schema and table creation are handled in the data_loading_sqlite.py file. I also exported data to Google BigQuery.

**Error Handling and Data Validation**: I implemented error handling for API requests, data validation for the API response structure, and custom exceptions for ETL-related errors.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Open Meteo API for providing weather data.
- Google Cloud for BigQuery integration.
- Python community for powerful libraries.
## Author
Juliana Nobile

julianaanobile@gmail.com

