#Merge script 002

import os
import pandas as pd
import logging

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Path to the directory containing the CSV files
directory_path2 = '/home/max/Desktop/Hiwi_Job/BON_LTE_160524_HUE_002'
dataframes2 = {}

# Load all CSV files into a dictionary of DataFrames
for filename in os.listdir(directory_path2):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory_path2, filename)
        dataframes2[filename] = pd.read_csv(file_path, low_memory=False)
        logging.info(f"Loaded {filename} with columns: {dataframes2[filename].columns.tolist()}")


def find_common_columns(df1, df2):
    """Find common columns between two DataFrames."""
    return list(set(df1.columns).intersection(set(df2.columns)))


def coerce_column_types(df1, df2, columns):
    """Coerce column types to be the same for merging."""
    for column in columns:
        if df1[column].dtype != df2[column].dtype:
            if pd.api.types.is_numeric_dtype(df1[column]) and pd.api.types.is_numeric_dtype(df2[column]):
                df1[column] = df1[column].astype(float)
                df2[column] = df2[column].astype(float)
            else:
                df1[column] = df1[column].astype(str)
                df2[column] = df2[column].astype(str)


def merge_dataframes(dataframes, start_key):
    """Merge all DataFrames iteratively based on the most common columns."""
    merged_df = dataframes.pop(start_key)
    logging.info(f"Starting merge with {start_key}")

    while dataframes:
        best_match_key = None
        best_common_columns = []
        for key, df in dataframes.items():
            common_columns = find_common_columns(merged_df, df)
            if len(common_columns) > len(best_common_columns):
                best_match_key = key
                best_common_columns = common_columns
        if not best_common_columns:
            raise ValueError("No common columns found for merging.")

        logging.info(f"Merging with {best_match_key} on columns: {best_common_columns}")

        # Ensure column types match for merging
        coerce_column_types(merged_df, dataframes[best_match_key], best_common_columns)

        merged_df = pd.merge(merged_df, dataframes.pop(best_match_key), on=best_common_columns, how='outer')

    return merged_df


def verify_merge(merged_df, original_dfs):
    """Verify if the merged DataFrame contains all unique columns from the original DataFrames."""
    # Collect all unique columns from original DataFrames
    unique_columns = set()
    for df in original_dfs.values():
        unique_columns.update(df.columns)

    # Check for missing columns
    missing_columns = unique_columns - set(merged_df.columns)
    if missing_columns:
        logging.warning(f"Missing columns in the final merged dataset: {missing_columns}")
    else:
        logging.info("All unique columns are present in the final merged dataset.")

    # Optionally, perform a sample data check (e.g., first row from each original DataFrame)
    for key, original_df in original_dfs.items():
        sample_row = original_df.iloc[0]
        for column in sample_row.index:
            if column in merged_df.columns:
                merged_values = merged_df.loc[merged_df[column] == sample_row[column], column]
                if not merged_values.empty and all(merged_values == sample_row[column]):
                    logging.info(f"Column {column} from {key} is correctly merged.")
                else:
                    logging.warning(f"Discrepancy found in column {column} from {key}.")
            else:
                logging.warning(f"Column {column} from {key} is missing in the merged dataset.")


# Start the merging process with 'Ertrag' DataFrame
start_key = 'lte_seehausen.ID_L0204_V1_0_ERTRAG.csv'
merged_data = merge_dataframes(dataframes2, start_key)

# Verify the merge
verify_merge(merged_data, dataframes2)

# Save the final merged DataFrame to a specified path
output_path = '/home/max/Desktop/Hiwi_Job/BON_LTE_160524_HUE_002/BON_LTE_160524_HUE_002_unified_final.csv'
merged_data.to_csv(output_path, index=False)
logging.info(f"Final merged data saved to '{output_path}'")



