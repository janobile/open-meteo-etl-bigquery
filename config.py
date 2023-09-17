import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API settings
API_BASE_URL = "https://api.open-meteo.com/v1"

# Database settings
SQLITE_DATABASE_PATH = "sqlite_database.db"
SQLITE_TABLE = 'weather_data'

# Google Cloud settings
DEFAULT_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DEFAULT_DATASET_ID = os.getenv("GCP_DATASET_ID")

# Logging settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # Default to INFO if not provided
