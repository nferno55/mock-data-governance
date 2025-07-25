import datetime
import sqlite3
import pandas as pd
import re
import os
from datetime import datetime

# Connect to SQLite database
conn = sqlite3.connect("mock_data.db")
# list of all fields
all_values = ("first_name", "last_name", "email", "gender", "account", "phone_number", "date_created")
# list of possible duplicate fields
values = ("first_name", "last_name", "email", "account", "phone_number")
# used to store all values where there are no issues so we do not print out blank JSON files
no_issues = []


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
        # print(f"No rows found for: {filename}")
        no_issues.append(f"No results found for: {filename}")


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
invalid_email_query = "SELECT * FROM mock_data WHERE email IS NOT NULL AND email NOT LIKE '%_@__%.__%';"
run_query_and_export(invalid_email_query, "invalid_email.csv", conn)

# Invalid phone numbers
invalid_phone_query = "SELECT * FROM mock_data WHERE phone_number IS NOT NULL AND LENGTH(phone_number) != 10;"
run_query_and_export(invalid_phone_query, "invalid_phone.csv", conn)

# Future dates
future_date_query = "SELECT * FROM mock_data WHERE date_created > DATE('now');"
run_query_and_export(future_date_query, "future_date.csv", conn)

with open("dq_summary_log.txt", "w") as log:
    log.write("DATA QUALITY CHECK SUMMARY\n")
    log.write("=" * 40 + "\n")
    if no_issues:
        for line in no_issues:
            log.write(line + "\n")
    else:
        log.write("All checks passed. No issues found.\n")
