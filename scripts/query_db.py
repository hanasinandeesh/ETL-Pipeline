import sqlite3

# Define the path to your SQLite database
db_path = r"C:\Users\hansi\OneDrive\Desktop\assessment\etl_project\db\iris_data.db"

def query_database(db_path):
    """Query the SQLite database to display the aggregated data."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query the database to retrieve data from the iris_aggregated table
        cursor.execute("SELECT * FROM iris_aggregated")

        # Fetch all rows from the query result
        rows = cursor.fetchall()

        # Display the results
        if rows:
            print("Data from the iris_aggregated table:")
            for row in rows:
                print(row)
        else:
            print("No data found in the database.")

        # Close the connection
        conn.close()
    except Exception as e:
        print(f"Error while querying the database: {e}")

if __name__ == "__main__":
    query_database(db_path)
