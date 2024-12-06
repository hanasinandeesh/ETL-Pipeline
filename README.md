# ETL Pipeline Project

## Overview
This project demonstrates an ETL (Extract, Transform, Load) pipeline implemented using Python and SQLite. The pipeline extracts data from a CSV file (Iris dataset), performs data transformations (such as filtering and aggregation), and loads the transformed data into both a CSV file and a SQLite database. The dataset used is the well-known Iris dataset, which contains measurements of sepals and petals of various Iris flower species.

## Objective
- **Extract**: Load the Iris dataset from a CSV file.
- **Transform**: Clean the dataset, filter it based on a condition, and compute the average petal length for each species.
- **Load**: Save the transformed data to a new CSV file and load it into a SQLite database.

## Images:
### 1. ETL Pipeline Successful Run:
Here is an image showing the successful run of the ETL pipeline:

<img width="864" alt="Screenshot 2024-12-07 010252" src="https://github.com/user-attachments/assets/26c64e04-0529-4071-8be8-979085218696">


### 2. Loaded Data Display:
This image shows the loaded data displayed from the SQLite database:

<img width="704" alt="Screenshot 2024-12-07 010639" src="https://github.com/user-attachments/assets/52abe68b-1318-4731-857b-d3a1c4108338">


### 3. Query and Data in SQLite:
Here is an image showing a query run on the SQLite database to retrieve the loaded data:

<img width="634" alt="Screenshot 2024-12-07 010537" src="https://github.com/user-attachments/assets/bee5814f-d456-4671-8cfb-43ab89dd280b">



## Technologies Used
- **Python**: Programming language used to implement the ETL pipeline.
- **Pandas**: Library used for data manipulation and transformation.
- **SQLite**: Database used for storing the transformed data.
- **SQLite3**: Python library to interact with SQLite databases.

## Dataset
The dataset used in this project is the Iris dataset. It contains the following columns:
- `sepal.length`: Sepal length in cm.
- `sepal.width`: Sepal width in cm.
- `petal.length`: Petal length in cm.
- `petal.width`: Petal width in cm.
- `variety`: Species of the Iris plant (Setosa, Versicolor, or Virginica).

### Source:
- **Original Dataset**: The Iris dataset is publicly available. You can find it on [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/iris).

## Features of the ETL Pipeline

1. **Data Extraction**: The pipeline reads data from the `iris.csv` file located in the `data/` folder.
2. **Data Transformation**: 
   - Remove missing values.
   - Filter rows where the petal length is greater than 1.5 cm.
   - Aggregate the average petal length by species.
3. **Data Loading**: 
   - The transformed data is saved to a new CSV file, `transformed_data.csv`.
   - The transformed data is also loaded into an SQLite database (`iris_data.db`), where it is stored in the `iris_aggregated` table.

## Getting Started

### Prerequisites
To run this ETL pipeline locally, you'll need Python and the following libraries:
- `pandas`
- `sqlite3`


