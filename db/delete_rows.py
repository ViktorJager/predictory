import sqlite3
from config.logger_setup import setup_logger

from config.db_config import COIN_DB_NAMES


logger = setup_logger()


def _delete_rows(db, start_date, end_date):
    # Connect to the SQLite database
    connection = sqlite3.connect(f"./db/{db}/{db}.sqlite")
    cursor = connection.cursor()

    # Specify the date range
    # start_date = '2023-05-25'
    # end_date = '2023-05-29'

    # Delete the rows within the specified date range
    cursor.execute(
        f"DELETE FROM {db} WHERE date BETWEEN ? AND ?", (start_date, end_date)
    )
    connection.commit()

    # Check the number of rows affected
    logger.info(f"Number of rows deleted in {db}: {cursor.rowcount}")

    # Close the database connection
    connection.close()


def delete_rows():
    for db in COIN_DB_NAMES.keys():
        _delete_rows(f"{db}", start_date="2023-06-12", end_date="2023-06-17")
