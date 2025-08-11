import yaml
import sqlite3
import pandas as pd
import re
from datetime import datetime

# pull un the metadata to parse through it
with open("metadata.yaml") as f:
    metadata = yaml.safe_load(f)
    print("YAML file loaded properly")


# check for a valid email using Regular Expression
def is_valid_email(email):
    ...
