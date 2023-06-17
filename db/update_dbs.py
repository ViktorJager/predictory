import csv
import sqlite3
from time import sleep
import requests
from config.logger_setup import setup_logger
from datetime import date, timedelta, datetime

from config.db_config import COIN_DB_NAMES, DATE_PATTERN, FG_DB_NAME


logger = setup_logger()


def update_coin_dbs(db, coin):
    date_list = missing_dates(db)

    if date_list:
        update_coin_db(db, coin, date_list)
    else:
        logger.info(f"{db} already up to date")


def update_other_dbs(db):
    date_list = missing_dates(db)

    if date_list:
        update_fg_db(db, date_list)


def missing_dates(db):
    # Connect to the database
    connection = sqlite3.connect(f"./db/{db}/{db}.sqlite")
    cursor = connection.cursor()

    # Query the last entry in the table
    query = f"SELECT * FROM {db} ORDER BY date DESC LIMIT 1"
    cursor.execute(query)
    last_entry = cursor.fetchone()

    # Get today's date
    today = date.today()
    date_list = []

    # Check if the last entry is not today's date
    if last_entry and last_entry[0] != today.strftime(DATE_PATTERN):
        # Get the date from the last entry
        last_date = datetime.strptime(last_entry[0], DATE_PATTERN).date()

        # Generate a list of dates between last_date and today
        current_date = last_date + timedelta(days=1)
        while current_date <= today:
            date_list.append(current_date.strftime(DATE_PATTERN))
            current_date += timedelta(days=1)

    connection.close()
    return date_list


def update_coin_db(db, coin, date_list):
    # CoinGecko API endpoint for fetching price history
    api_url = f"https://api.coingecko.com/api/v3/coins/{coin}/history"

    # Connect to the database
    connection = sqlite3.connect(f"./db/{db}/{db}.sqlite")
    cursor = connection.cursor()

    # Iterate over the dates and fetch the prices
    for this_date in date_list:
        date_obj = datetime.strptime(this_date, "%Y-%m-%d")
        reversed_date = date_obj.strftime("%d-%m-%Y")
        # Make an API request to CoinGecko
        params = {"id": f"{db}", "date": reversed_date}
        response = requests.get(api_url, params=params)

        if response.status_code == 429:
            logger.info("API Rate limit exceeded. Sleeping for 1 min.")
            sleep(61)
            response = requests.get(api_url, params=params)

        # Parse the response and retrieve the price
        price_data = response.json()
        price = price_data.get("market_data", {}).get("current_price", {}).get("usd")

        if not price is None:
            logger.info(f"Added row in {db}: {this_date} {price}")
            cursor.execute(
                f"INSERT INTO {db} (date, price) VALUES (?, ?)", (this_date, price)
            )
            connection.commit()
        else:
            logger.info(f"Skip {db} - Invalid price ({this_date}, {price})")

    # Close the database connection
    connection.close()


def update_fg_db(db, date_list):
    connection = sqlite3.connect(f"./db/{db}/{db}.sqlite")
    cursor = connection.cursor()

    url = "https://api.alternative.me/fng/"
    params = {"limit": len(date_list) + 2, "format": "csv"}

    response = requests.get(url, params=params)
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
    else:
        logger.info("Request failed with status code:", response.status_code)
        exit(0)

    data = filter_csv(csv_data)

    # Iterate over the dates to update
    for this_date in date_list:
        # Check if the date already exists in the database
        cursor.execute(f"SELECT COUNT(*) FROM {db} WHERE date = ?", (this_date,))
        count = cursor.fetchone()[0]

        if count == 0:
            # Find the corresponding data from the API response
            for api_data in data:
                if api_data[0] == this_date:
                    value = api_data[1]
                    # Insert the date and price into the database
                    cursor.execute(
                        f"INSERT INTO {db} (date, value) VALUES (?, ?)",
                        (this_date, value),
                    )
                    logger.info(f"Added row: {this_date} {value}")
                    break

    connection.commit()
    connection.close()


def filter_csv(data):
    new_date_list = []

    for entry in data:
        date_str = entry[0]
        new_date = datetime.strptime(date_str, "%d-%m-%Y").strftime(DATE_PATTERN)
        modified_entry = [new_date, entry[1]]  # Exclude the third entry
        new_date_list.append(modified_entry)
    return new_date_list


def run_update_db():
    logger.info("Init DB update")

    for db, coin in COIN_DB_NAMES.items():
        update_coin_dbs(db, coin)

    update_other_dbs(FG_DB_NAME)

    logger.info("DB update complete")
