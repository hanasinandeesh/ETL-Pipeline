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
        # Read the CSV file into a DataFrame
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

        # Remove rows with missing data
        df_cleaned = df.dropna()

        # Filter rows where petal.length > 1.5
        df_filtered = df_cleaned[df_cleaned['petal.length'] > 1.5]

        # Aggregate: Average petal length by species
        df_aggregated = df_filtered.groupby('variety', as_index=False)['petal.length'].mean()
        df_aggregated.rename(columns={'petal.length': 'avg_petal_length'}, inplace=True)

        print("Data transformation successful.")
        return df_aggregated
    except Exception as e:
        raise Exception(f"Error during data transformation: {e}")

def load(df, transformed_path, db_path):
    """Load data into a SQLite database and save the transformed CSV."""
    print("Loading data into SQLite database and saving transformed CSV...")
    try:
        # Save transformed data into CSV
        df.to_csv(transformed_path, index=False)
        print(f"Transformed data saved to {transformed_path}")

        # Load data into SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Drop and recreate the table
        cursor.execute("DROP TABLE IF EXISTS iris_aggregated")
        cursor.execute('''
        CREATE TABLE iris_aggregated (
            species TEXT PRIMARY KEY,
            avg_petal_length REAL
        )
        ''')

        # Insert the transformed data into the database
        df.to_sql('iris_aggregated', conn, if_exists='replace', index=False)

        # Verify the data
        print("Data loaded into the database. Verifying:")
        for row in cursor.execute("SELECT * FROM iris_aggregated"):
            print(row)

        conn.commit()
        conn.close()
        print("Data loading successful.")
    except Exception as e:
        raise Exception(f"Error during data loading: {e}")

def main():
    """Main function to execute the ETL pipeline."""
    try:
        # Extract the dataset
        df = extract(data_path)

        # Transform the dataset
        transformed_df = transform(df)

        # Load the transformed data into the database and save as CSV
        load(transformed_df, transformed_path, db_path)

        print("ETL pipeline completed successfully!")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")

# Run the ETL pipeline
if __name__ == "__main__":
    main()
