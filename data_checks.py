import datetime
import sqlite3
import pandas as pd
import re
import os

# Connect to SQLite database
conn = sqlite3.connect("mock_data.db")

# list of all fields
all_values = ("first_name", "last_name", "email", "gender", "account", "phone_number", "date_created")


# looking for NULL values
for text in all_values:
    query = f"SELECT * FROM mock_data WHERE {text} IS NULL;"
    # Read into a DataFrame
    df = pd.read_sql_query(query, conn)

    df.to_csv(f"null_{text}.csv", index=False)
    print(f"Exported null check for {text}")

# list of possible duplicate fields
values = ("first_name", "last_name", "email", "account", "phone_number")

# looking for DUPLICATES, ignoring gender and date_created
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
