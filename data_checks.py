import sqlite3
import pandas as pd
import re
import os
from datetime import datetime

# Define output folder name
output_dir = "output"
# Create the folder if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
# variable to store the database inside - allows for easy transfer to new database with minimal changes to code
db_name = "mock_data.db"
database = db_name.replace('.db', '')
# Connect to SQLite database
conn = sqlite3.connect(db_name)
# list of all fields
all_values = ("first_name", "last_name", "email", "gender", "account", "phone_number", "date_created")
# list of possible duplicate fields
values = ("first_name", "last_name", "email", "account", "phone_number")
# used to store all values where there are no issues, so we do not print out blank CSV files
no_issues = []

# ensuring the no_issues list is clear, especially if the script gets run multiple times
no_issues.clear()  # ensure list starts empty regardless of how many consecutive times the script is ran


# adds results from queries that are good - ie no duplicates, nulls, or missing values
def log_no_issues(message):
    # simple 
    no_issues.append(message)


# TODO: add functions to correct issues after finding them, utilizing ETL and CI/CD methodologies

# function to compile all the contents of no_issues into a simple txt file for review
def write_summary_log(filename="dq_summary_log.txt"):
    full_path = os.path.join(output_dir, filename)
    # using  w for now; we do not care about appending and keeping the data from previous runs right now
    # maybe a future addition
    with open(full_path, "w") as log:
        log.write("DATA QUALITY CHECK SUMMARY\n")
        log.write("=" * 40 + "\n")
        if no_issues:
            for line in no_issues:
                # we do not need to use writelines since we are looping through each element of no_issues with /n
                log.write(line + "\n")
        else:
            # simple all good text written to the file if no issues are found
            log.write("All checks passed. No issues found.\n")
    no_issues.clear()  # Clear after writing, ready for next run


# check for incorrect data, utilizing a single function for multiple uses
#  query is the actual SQL query used, filename is used to name the corresponding csv file, conn is the database
def run_query_and_export(query: str, filename: str, conn: sqlite3.Connection):
    # Runs the SQL query, writes the result to a CSV file if rows are returned.
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        # creates a csv file with the relevant query for review
        # Save CSV inside output folder
        full_path = os.path.join(output_dir, filename)
        df.to_csv(full_path, index=False)
        print(f"Exported: {full_path}")
    else:
        # call the function instead of directly writing to the list
        log_no_issues(f"No results found for: {filename}")


# NULL value checks - runs through all possible fields looking for null values
for field in all_values:
    query = f"SELECT * FROM {database} WHERE {field} IS NULL;"
    run_query_and_export(query, f"null_{field}.csv", conn)

# Duplicate checks - simply shows duplicates of certain fields for review to ensure no incorrect values were entered
for field in values:
    query = f"""
        SELECT {field}, COUNT(*) as count
        FROM {database}
        WHERE {field} IS NOT NULL
        GROUP BY {field}
        HAVING count > 1;
    """
    run_query_and_export(query, f"duplicate_{field}.csv", conn)

# Invalid email format
invalid_email_query = f"SELECT * FROM {database} WHERE email IS NOT NULL AND email NOT LIKE '%_@__%.__%';"
run_query_and_export(invalid_email_query, "invalid_email.csv", conn)

# Invalid phone numbers - they must include the area code, but not the 1 in front of it
# This will also flag numbers that DO include the 1, but an exception can be made to those at a later date if needed
invalid_phone_query = f"SELECT * FROM {database} WHERE phone_number IS NOT NULL AND LENGTH(phone_number) != 10;"
run_query_and_export(invalid_phone_query, "invalid_phone.csv", conn)

# Future dates
# substr changes mm/dd/yyyy into a format that Date('now') can use - yyyy/mm/dd; first value
# determines starting position and second determines the length of the value to use
future_date_query = f"""
    SELECT *
    FROM {database}
    WHERE DATE(substr(date_created, 7, 4) || '-' || substr(date_created, 1, 2) || '-' || substr(date_created, 4, 2))
     > DATE('now');
"""
run_query_and_export(future_date_query, "future_date.csv", conn)

write_summary_log()

# preparations for including everything inside a main function to run until prompted to exit
# not implemented for this current version

# if __name__ == "__main__":
#     main()
