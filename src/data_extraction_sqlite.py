import sqlite3
from src.config import (
    SQLITE_TABLE,
    SQLITE_DATABASE_PATH,
)

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