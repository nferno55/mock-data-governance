import datetime
import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("mock_data.db")

# Create a list of queries that will run automatically and output their results into CSV files
query = (
    """
        SELECT * FROM mock_data WHERE first_name IS NULL;
        """,
    """
         SELECT * FROM mock_data WHERE last_name IS NULL;
         """,
    """
         SELECT * FROM mock_data WHERE email IS NULL;
         """,
    """
         SELECT * FROM mock_data WHERE gender IS NULL;
         """,
    """
         SELECT * FROM mock_data WHERE phone_number IS NULL;
         """,
    """
         SELECT * FROM mock_data WHERE account IS NULL;
         """,
    """
         SELECT * FROM mock_data WHERE date_created IS NULL;
         """)

for x, y in enumerate(query):
    # Read into a DataFrame
    current_query = y
    print(x)
    df = pd.read_sql_query(current_query, conn)

    # Export to CSV
    df.to_csv(f"query_{x}.csv", index=False)

    print(f"Exported query {x} to .csv")
