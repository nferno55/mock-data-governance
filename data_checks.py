import datetime
import sqlite3
import pandas as pd
import re
import os

# Connect to SQLite database
conn = sqlite3.connect("mock_data.db")

# list of possible duplicate fields
all_values = ("first_name", "last_name", "email", "account", "phone_number")

# Create a list of queries that will run automatically and output their results into CSV files
# List of queries to run
queries = (
    "SELECT * FROM mock_data WHERE first_name IS NULL;",
    "SELECT * FROM mock_data WHERE last_name IS NULL;",
    "SELECT * FROM mock_data WHERE email IS NULL;",
    "SELECT * FROM mock_data WHERE gender IS NULL;",
    "SELECT * FROM mock_data WHERE phone_number IS NULL;",
    "SELECT * FROM mock_data WHERE account IS NULL;",
    "SELECT * FROM mock_data WHERE date_created IS NULL;",
)

# looking for NULL values
for text in all_values:
    query = f"SELECT * FROM mock_data WHERE {text} IS NULL;"
    # Read into a DataFrame
    df = pd.read_sql_query(query, conn)

    df.to_csv(f"null_{text}.csv", index=False)
    print(f"Exported null check for {text}")

# list of possible duplicate fields
values = ("first_name", "last_name", "email", "account", "phone_number")

for column in values:
    query = f"""
        SELECT {column}, COUNT(*) as count
        FROM mock_data
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        HAVING count > 1;
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv(f"duplicate_{column}.csv", index=False)
    print(f"Exported duplicate check for {column}")

# check for incorrect data
