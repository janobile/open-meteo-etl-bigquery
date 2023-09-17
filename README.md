# Open Meteo ETL with BigQuery Integration

This project is an ETL (Extract, Transform, Load) pipeline built in Python that extracts weather data from the Open Meteo API, transforms the data, and loads it into both a local SQLite database and Google BigQuery for further analysis.

## Prerequisites

Before you start using this project, ensure that you have the following prerequisites in place:

- Google Cloud Platform (GCP) Account: You must have a GCP account to access BigQuery.
- Service Account Credentials: Obtain service account credentials in JSON format for your GCP project. These credentials are required for authentication and authorization when working with BigQuery. Make sure to grant appropriate permissions to the service account, including BigQuery access.
- Python (3.x recommended)
- Required Python libraries (see `requirements.txt`)

## Getting Started

1. Clone this repository to your local machine:

    ```
    git clone https://github.com/your-username/open-meteo-etl-bigquery.git
    cd open-meteo-etl-bigquery
    ```
2. Create and activate a virtual environment (optional but recommended):

    ```
    python -m venv venv
    source venv/bin/activate  # On Windows, use: venv\Scripts\activate
    ```

3. Set up your Google Cloud credentials by placing your JSON key file in the project directory and updating the `.env` file:
    ```
    GOOGLE_APPLICATION_CREDENTIALS="./credentials.json"
    ```
4. Install project dependencies:

    ```
    pip install -r requirements.txt
    ```
5. Run the ETL process:
    ```
    python main.py
    ```
## Configuration

Before running the project, ensure that you have configured the necessary settings and environment variables. You can find these settings in the `config.py` file:

```python
# config.py

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DEFAULT_PROJECT_ID = "your-project-id"
DEFAULT_DATASET_ID = "your-dataset"
SQLITE_TABLE = 'weather_data'
```
Additionally, you need to set up the `.env` file with the following content:
```textplain
VIRTUAL_ENV=venv
GOOGLE_APPLICATION_CREDENTIALS="./credentials.json"
```
- `VIRTUAL_ENV`: Specifies the virtual environment name. You can customize this to match your environment setup.

- `GOOGLE_APPLICATION_CREDENTIALS`: Points to the path of your Google Cloud credentials JSON file. Ensure this path is correct.

Make sure that the `.env` file and the `config.py` settings are correctly configured before running the project.


# Directory Structure
```bash
.
├── open-meteo-etl-bigquery/    # Main project directory.
│   ├── __init__.py             # Python package initialization.
│   ├── .env                    # Environment configuration for Google Cloud credentials.
│   ├── config.py               # Configuration constants for the project.
│   ├── credentials.json        # Google Cloud credentials JSON file.
│   ├── requirements.txt        # Dependency list for the project.
│   ├── sqlite_database.db      # Local SQLite database file.
│   ├── LICENSE                 # MIT License for the project.
│   ├── main.py                 # Main script containing the ETL process.
│   ├── README.md               # This documentation file.
│   └── venv/                   # Virtual environment directory (created when using venv).
```

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Open Meteo API for providing weather data.
- Google Cloud for BigQuery integration.
- Python community for powerful libraries.
## Author
Juliana Nobile
julianaanobile@gmail.com

