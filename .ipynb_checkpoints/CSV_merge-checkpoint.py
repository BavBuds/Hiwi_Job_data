import os
import pandas as pd

# Path to the directory containing CSV files
directory_path = '/home/max/Desktop/Hiwi_Job/BON_ENY_100524_HEI_001'

# Dictionary to hold dataframes
dataframes = {}

# Loop through all files in the directory
for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):  # check if the file is a CSV
        file_path = os.path.join(directory_path, filename)  # full path to the file
        dataframes[filename] = pd.read_csv(file_path)  # read the file and store it in the dictionary

# Now 'dataframes' dictionary contains all the CSV files as dataframes
# Loop through each key (filename) and value (DataFrame) in the dataframes dictionary
for filename, df in dataframes.items():
    print(f"Contents of {filename}:")
    print("Columns:", df.columns.tolist())  # Show column names
    print("First few rows:")
    print(df.head())  # Display the first few rows of the DataFrame
    print("\n")  # Add a newline for better separation between files
