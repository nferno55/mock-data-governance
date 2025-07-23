import datetime
import sqlite3
import pandas as pd
import re
import os

# Connect to SQLite database
conn = sqlite3.connect("mock_data.db")

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
for query in queries:
    print(f"Running current query; {query}")
    # Read into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Extract condition from WHERE clause for filename
    match = re.search(r'WHERE\s+(.+?);?$', query, re.IGNORECASE)
    if match:
        condition = match.group(1)
        filename_part = re.sub(r'\W+', '_', condition.strip().lower())  # sanitize
    else:
        filename_part = f"query_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    filename = f"{filename_part}.csv"

    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"Exported to {filename}\n")

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