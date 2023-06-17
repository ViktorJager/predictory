import requests
import csv
import sqlite3
from datetime import date, timedelta, datetime

from config.db_config import FG_DB_NAME


date_pattern = "%Y-%m-%d"

# Connect to the database
connection = sqlite3.connect(f"./db/{FG_DB_NAME}/{FG_DB_NAME}.sqlite")
cursor = connection.cursor()

# Query the last entry in the table
query = f"SELECT date FROM {FG_DB_NAME} ORDER BY date DESC LIMIT 1"
cursor.execute(query)
last_entry = cursor.fetchone()

# Get today's date
today = date.today()
date_list = []

# Check if the last entry is not today's date
if last_entry and last_entry[0] != today.strftime(date_pattern):
    # Get the date from the last entry
    last_date = datetime.strptime(last_entry[0], date_pattern).date()

    # Generate a list of dates between last_date and today
    current_date = last_date + timedelta(days=1)
    while current_date <= today:
        date_list.append(current_date.strftime(date_pattern))
        current_date += timedelta(days=1)

else:
    connection.close()
    exit(0)


url = "https://api.alternative.me/fng/"
params = {"limit": len(date_list) + 2, "format": "csv"}

response = requests.get(url, params=params)

# Create a dictionary to store the CSV data
csv_data = []

if response.status_code == 200:
    text_data = response.text

    # Use a CSV reader to read the text data
    reader = csv.reader(text_data.splitlines())

    # Skip the header row
    next(reader)

    # Iterate over the remaining rows
    for row in reader:
        if len(row) == 3:
            csv_data.append(row)

    csv_data = csv_data[1:]

    # print(csv_data)
else:
    print("Request failed with status code:", response.status_code)
    exit(0)


def filter_csv(data):
    new_date_list = []

    for entry in data:
        date_str = entry[0]
        new_date = datetime.strptime(date_str, "%d-%m-%Y").strftime(date_pattern)
        modified_entry = [new_date, entry[1]]  # Exclude the third entry
        new_date_list.append(modified_entry)
    return new_date_list


data = filter_csv(csv_data)


# Iterate over the dates to update
for this_date in date_list:
    # Check if the date already exists in the database
    cursor.execute(f"SELECT COUNT(*) FROM {FG_DB_NAME} WHERE date = ?", (this_date,))
    count = cursor.fetchone()[0]

    if count == 0:
        # Find the corresponding data from the API response
        for api_data in data:
            if api_data[0] == this_date:
                value = api_data[1]
                # Insert the date and price into the database
                cursor.execute(
                    f"INSERT INTO {FG_DB_NAME} (date, value) VALUES (?, ?)",
                    (this_date, value),
                )
                print(f"Added row: {this_date} {value}")
                break


connection.commit()
connection.close()
