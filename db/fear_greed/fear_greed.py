import requests
import csv
import datetime
import sqlite3

url = "https://api.alternative.me/fng/"
params = {
    "limit": "0",
    "format": "csv"
}

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

    print(csv_data)
else:
    print("Request failed with status code:", response.status_code)

def filter_csv(data):
    new_date_list = []

    for entry in data:
        date_str = entry[0]
        new_date = datetime.datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
        modified_entry = [new_date, entry[1]]  # Exclude the third entry
        new_date_list.append(modified_entry)
    return new_date_list


data = filter_csv(csv_data)


db_file = './db/fear-greed/fear_greed.sqlite'
connection = sqlite3.connect(db_file)
cursor = connection.cursor()

table_name = 'fear_greed'
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date TEXT, value INTEGER)")

# Iterate over the entries
for entry in data:
    date = entry[0]
    value = entry[1]
    cursor.execute(f"INSERT INTO {table_name} (date, value) VALUES (?, ?)", (date, value))

connection.commit()
connection.close()
