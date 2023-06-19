import requests
import sqlite3
from datetime import datetime, date
from config.logger_setup import setup_logger
from config.db_config import DATE_PATTERN, COINRANKING_DB_NAME

from api_keys import COINRANKING_API_KEY


logger = setup_logger()


def update_coinranking_db():
    # Connect to the database
    connection = sqlite3.connect(f"./db/{COINRANKING_DB_NAME}/{COINRANKING_DB_NAME}.sqlite")
    cursor = connection.cursor()

    # Query the last entry in the table
    query = f"SELECT date FROM {COINRANKING_DB_NAME} ORDER BY date DESC LIMIT 1"
    cursor.execute(query)
    last_entry = cursor.fetchone()

    today = date.today()

    # Check if the last entry is not today's date
    if last_entry and last_entry[0] != today.strftime(DATE_PATTERN):
        headers = {'x-access-token': COINRANKING_API_KEY}
        response = requests.request("GET", "https://api.coinranking.com/v2/stats", headers=headers)

        response_json = response.json() if response.status_code == 200 else {}

        data = {
            "totalCoins": response_json.get("data", {}).get("totalCoins"),
            "totalMarkets": response_json.get("data", {}).get("totalMarkets"),
            "totalExchanges": response_json.get("data", {}).get("totalExchanges"),
            "totalMarketCap": response_json.get("data", {}).get("totalMarketCap"),
            "total24hVolume": response_json.get("data", {}).get("total24hVolume"),
            "btcDominance": response_json.get("data", {}).get("btcDominance")
        }

        data["date_today"] = datetime.now().strftime(DATE_PATTERN)


        db_file = './db/coinranking/coinranking.sqlite'
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()

        table_name = 'coinranking'
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (date TEXT, totalCoins INTEGER, totalMarkets INTEGER, totalExchanges INTEGER, totalMarketCap TEXT, total24hVolume TEXT, btcDominance FLOAT)")


        cursor.execute(f"INSERT INTO {table_name} (date, totalCoins, totalMarkets, totalExchanges, totalMarketCap, total24hVolume, btcDominance) VALUES (?, ?, ?, ?, ?, ?, ?)", (data["date_today"], data["totalCoins"], data["totalMarkets"], data["totalExchanges"], data["totalMarketCap"], data["total24hVolume"], data["btcDominance"]))

        connection.commit()
        connection.close()
        logger.info(f"Updated {COINRANKING_DB_NAME} db")
    else:
        logger.info(f"{COINRANKING_DB_NAME} already up to date")
