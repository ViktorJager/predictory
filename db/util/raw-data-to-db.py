import os
import sqlite3
import pandas as pd
from datetime import datetime

from config import DATE_PATTERN



def raw_data_to_btc(table_name):

    csv_file = f'./db/{table_name}/raw-data/{table_name}.csv'
    db_file = f'./db/{table_name}/{table_name}.sqlite'

    # Check if the SQLite file already exists
    if not os.path.exists(db_file):
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        open(db_file, 'w').close()

    df = pd.read_csv(csv_file, low_memory=False)

    # Filter rows based on date range
    start_date = datetime(2017, 1, 1)
    end_date = datetime.now()
    df['time'] = pd.to_datetime(df['time'])
    df = df[(df['time'] >= start_date) & (df['time'] <= end_date)]

    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date TEXT, price REAL)")

    # Iterate over the filtered rows
    for _, row in df.iterrows():
        date = row['time'].strftime(DATE_PATTERN)
        price = row['PriceUSD']
        cursor.execute(f"INSERT INTO {table_name} (date, price) VALUES (?, ?)", (date, price))

    connection.commit()
    connection.close()




raw_data_to_btc("eth")