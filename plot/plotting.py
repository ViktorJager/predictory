import sqlite3
import matplotlib.pyplot as plt
import numpy as np

DB_NAME = "fear_greed"
TABLE_NAME = "fear_greed"

# Connect to the SQLite database
connection = sqlite3.connect(f"./db/{DB_NAME}/{DB_NAME}.sqlite")
cursor = connection.cursor()

# Execute the SELECT query
query = f"SELECT * FROM {TABLE_NAME} ORDER BY date DESC LIMIT 30;"
cursor.execute(query)

# Fetch the results and store them
results = cursor.fetchall()

# Extract the relevant data into separate lists
dates = [str(row[0]).split('-')[-1] for row in results]
values = [row[1] for row in results]

# Close the database connection
cursor.close()
connection.close()

# window_size = 3
# weights = np.repeat(1.0, window_size) / window_size
# smoothed_values = np.convolve(values, weights, 'valid')
# smoothed_dates = dates[window_size-1:]

# Create a line plot using Matplotlib
plt.plot(dates, values)
plt.xlabel('Date')
plt.ylabel('Value')
plt.ylim(1, 100)
plt.title('Last 30 Entries Sorted by Date')
plt.show()
