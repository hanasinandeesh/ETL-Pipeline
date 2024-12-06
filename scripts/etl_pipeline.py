import os
import pandas as pd
import sqlite3

# Define paths directly based on the provided location
data_path = r"C:\Users\hansi\OneDrive\Desktop\assessment\etl_project\data\iris.csv"
transformed_path = r"C:\Users\hansi\OneDrive\Desktop\assessment\etl_project\data\transformed_data.csv"
db_path = r"C:\Users\hansi\OneDrive\Desktop\assessment\etl_project\db\iris_data.db"

def extract(data_path):
    """Extract data from a CSV file."""
    print("Extracting data from CSV...")
    try:
        df = pd.read_csv(data_path)
        print("Data extraction successful.")
        print("Columns in the dataset:", df.columns)  # Log columns to check
        return df
    except FileNotFoundError:
        raise Exception(f"File not found at path: {data_path}")
    except Exception as e:
        raise Exception(f"Error during data extraction: {e}")

def transform(df):
    """Transform data: clean, filter, and aggregate."""
    print("Transforming data...")
    try:
        # Check if 'petal.length' column exists
        if 'petal.length' not in df.columns:
            raise Exception("'petal.length' column not found in the dataset.")

        # Remove missing values
        df_cleaned = df.dropna()

        # Filter rows where petal.length > 1.5
        df_filtered = df_cleaned[df_cleaned['petal.length'] > 1.5]

        # Perform different aggregations
        df_aggregated = df_filtered.groupby('variety').agg(
            total_petal_length=('petal.length', 'sum'),
            avg_petal_length=('petal.length', 'mean'),
            count=('petal.length', 'count'),
            max_petal_length=('petal.length', 'max'),
            min_petal_length=('petal.length', 'min'),
            std_petal_length=('petal.length', 'std')
        ).reset_index()

        print("Data transformation successful.")
        return df_aggregated
    except Exception as e:
        raise Exception(f"Error during data transformation: {e}")

def load(df, transformed_path, db_path):
    """Load data into a SQLite database and save the transformed CSV."""
    print("Loading data into SQLite database and saving transformed CSV...")
    try:
        # Save to transformed CSV
        df.to_csv(transformed_path, index=False)
        print(f"Transformed data saved to {transformed_path}")

        # Load into SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Drop and recreate the table
        cursor.execute("DROP TABLE IF EXISTS iris_aggregated")
        cursor.execute('''
        CREATE TABLE iris_aggregated (
            species TEXT PRIMARY KEY,
            total_petal_length REAL,
            avg_petal_length REAL,
            count INTEGER,
            max_petal_length REAL,
            min_petal_length REAL,
            std_petal_length REAL
        )
        ''')

        # Insert data into SQLite table
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO iris_aggregated (species, total_petal_length, avg_petal_length, count, max_petal_length, min_petal_length, std_petal_length) VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (row['variety'], row['total_petal_length'], row['avg_petal_length'], row['count'], row['max_petal_length'], row['min_petal_length'], row['std_petal_length']))

        # Verify the data with cleaner output format
        print("Data loaded into the database. Verifying:")
        cursor.execute("SELECT * FROM iris_aggregated")
        rows = cursor.fetchall()
        
        # Display the data in a clean, formatted way
        print(f"{'Species':<15} {'Total Petal Length':<20} {'Avg Petal Length':<20} {'Count':<10} {'Max Petal Length':<20} {'Min Petal Length':<20} {'Std Petal Length':<20}")
        print("-" * 130)  # Separator for better readability
        for row in rows:
            print(f"{row[0]:<15} {row[1]:<20} {row[2]:<20} {row[3]:<10} {row[4]:<20} {row[5]:<20} {row[6]:<20}")

        conn.commit()
        conn.close()
        print("Data loading successful.")
    except Exception as e:
        raise Exception(f"Error during data loading: {e}")

def main():
    """Main function to execute the ETL pipeline."""
    try:
        # Extract
        df = extract(data_path)

        # Transform
        transformed_df = transform(df)

        # Load
        load(transformed_df, transformed_path, db_path)

        print("ETL pipeline completed successfully!")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()
