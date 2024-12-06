import sqlite3

# Database path
db_path = r"C:\Users\hansi\OneDrive\Desktop\assessment\etl_project\db\iris_data.db"

try:
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all rows from the 'iris_aggregated' table
    cursor.execute("SELECT * FROM iris_aggregated")
    rows = cursor.fetchall()

    # Print the results
    print("Data from 'iris_aggregated' table:")
    for row in rows:
        print(row)

    # Close the connection
    conn.close()

except Exception as e:
    print(f"Error while querying the database: {e}")
