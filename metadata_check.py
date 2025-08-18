import yaml
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
cursor = conn.cursor()
# create a storage variable for any issues we come across
report = []

report.clear()  # Clear before each running of the script


# int checking function
def is_int(value):
    try: return str(value).isdigit()
    except: return False
def is_phone(value):
    # value.replace('-', '')
    return bool(re.match(r"^(?:\D*\d){12}\D$", str(value)))
def is_date(value):
    try:
        datetime.strptime(value, "%m-%d-%Y"); return True
    except:
        return False

# pull in the metadata to parse through it
with open("metadata.yaml") as f:
    metadata = yaml.safe_load(f)
    print("YAML file loaded properly")


# check for a valid email using Regular Expression
# def is_valid_email(email: str, md_type) -> bool:
#     print('checking for valid emails using regex')
for field in metadata['spec']['fields']:
    name = field['name']
    print("Field", name)
    type_expected = field['data_type']
    is_required = field.get('required', False)
    is_unique = field.get('unique', False)
    allowed_values = field.get('allowed_values', None)
    format = field.get('format', None)
    # "SELECT name FROM fields WHERE name=?", (name, )
    cursor.execute(f"SELECT id, {name} FROM {database}")
    values = [row[1] for row in cursor.fetchall()]
    # used to get the exact row where we have issues
    row_ids = [row[0] for row in cursor.fetchall()]


    # check for required
    if is_required and any(v is None or v == '' for v in values):
        report.append(f"Field '[{row_ids} {name}]' is missing required values.")
    # check types by typecasting
    if type_expected == "integer":
        if not all(is_int(v) for v in values):
            report.append(f"Field '[{row_ids} {name}]' must be an integer.")
    elif type_expected == "string":
        if format == "date":
            if not is_date(values[0]):
                report.append(f"Field '[{row_ids} {name}]' date format is invalid.")
        elif format == "phone":
            if not all(is_phone(v) for v in values):
                report.append(f"Field '[{row_ids} {name}]' Phone number format is invalid.")
    # ensure unique fields are unique
    if is_unique and len(set(values)) != len(values):
        report.append(f"Field '[{row_ids} {name}]' must be unique.")
    # check that allowed values are used
    if allowed_values:
        if not all(v in allowed_values or v is None for v in values):
            report.append(f"Field '[{row_ids} {name}]' must be in {allowed_values}.")

# create report at the end
if report:
    filename="metadata checks.txt"
    full_path = os.path.join(output_dir, filename)
    # using  w for now; we do not care about appending and keeping the data from previous runs right now
    # maybe a future addition though
    with open(full_path, "w") as log:
        log.write("METADATA ISSUES FOUND\n")
        log.write("=" * 40 + "\n")
        # log.write(f"Total entries inside current database: {total_entries}" + "\n")
        for line in report:
            # we do not need to use writelines since we are looping through each element of no_issues with /n
            log.write(line + "\n")
else:
    # simple all good text written to the file if no issues are found
    log.write("All metadata checks passed. No issues found.\n")
    report.clear()  # Clear after writing, ready for next run
print('Finished checking Metadata')
conn.close()