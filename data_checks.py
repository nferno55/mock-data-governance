import datetime
import sqlite3
import pandas as pd
import re
import os

# Connect to SQLite database
conn = sqlite3.connect("mock_data.db")
# list of all fields
all_values = ("first_name", "last_name", "email", "gender", "account", "phone_number", "date_created")
# list of possible duplicate fields
values = ("first_name", "last_name", "email", "account", "phone_number")


# check for incorrect data, utilizing a single function that can handle multiple types of queries
def run_query_and_export(query: str, filename: str, conn: sqlite3.Connection):
    """
    Runs the SQL query, writes the result to a CSV file if rows are returned.
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df.to_csv(filename, index=False)
        print(f"Exported: {filename}")
    else:
        print(f"No rows found for: {filename}")


# # looking for NULL values
# for text in all_values:
#     query = f"SELECT * FROM mock_data WHERE {text} IS NULL;"
#     # Read into a DataFrame
#     df = pd.read_sql_query(query, conn)
#
#     df.to_csv(f"null_{text}.csv", index=False)
#     print(f"Exported null check for {text}")
#
# # looking for DUPLICATES, ignoring gender and date_created
# for column in values:
#     query = f"""
#         SELECT {column}, COUNT(*) as count
#         FROM mock_data
#         WHERE {column} IS NOT NULL
#         GROUP BY {column}
#         HAVING count > 1;
#     """
#     df = pd.read_sql_query(query, conn)
#     df.to_csv(f"duplicate_{column}.csv", index=False)
#     print(f"Exported duplicate check for {column}")

# NULL value checks
for field in all_values:
    query = f"SELECT * FROM mock_data WHERE {field} IS NULL;"
    run_query_and_export(query, f"null_{field}.csv", conn)

# Duplicate checks
for field in values:
    query = f"""
        SELECT {field}, COUNT(*) as count
        FROM mock_data
        WHERE {field} IS NOT NULL
        GROUP BY {field}
        HAVING count > 1;
    """
    run_query_and_export(query, f"duplicate_{field}.csv", conn)

# Invalid email format
invalid_email_query = """
    SELECT * FROM mock_data
    WHERE email IS NOT NULL AND email NOT LIKE '%_@__%.__%';
"""
run_query_and_export(invalid_email_query, "invalid_email.csv", conn)

# Invalid phone numbers
invalid_phone_query = """
    SELECT * FROM mock_data
    WHERE phone_number IS NOT NULL AND LENGTH(phone_number) != 10;
"""
run_query_and_export(invalid_phone_query, "invalid_phone.csv", conn)

# Future dates
future_date_query = """
    SELECT * FROM mock_data
    WHERE date_created > DATE('now');
"""
run_query_and_export(future_date_query, "future_date.csv", conn)
