import sqlite3

DB = "fear_greed"

# Connect to the SQLite database
connection = sqlite3.connect(f'./db/{DB}/{DB}.sqlite')
cursor = connection.cursor()

# Rename the table
old_table_name = "fear-greed"
new_table_name = "fear_greed"
cursor.execute(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}")

# Commit the changes and close the connection
connection.commit()
connection.close()