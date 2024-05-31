import os
import pandas as pd
import gc
import logging
import random

# Configure logging
log_file = '/home/max/Desktop/Hiwi_Job/BON_LTE_160524_HUE_002/merge_quality_control.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file), logging.StreamHandler()])


def load_csv_files(directory_path, exclude_file):
    dataframes = {}
    try:
        for filename in os.listdir(directory_path):
            if filename.endswith('.csv') and filename != exclude_file:
                file_path = os.path.join(directory_path, filename)
                dataframes[filename] = pd.read_csv(file_path)
                logging.info(f"Loaded {filename} with shape {dataframes[filename].shape}")
    except Exception as e:
        logging.error(f"Error loading CSV files: {e}")
    return dataframes


def print_column_names(dataframes):
    for filename, df in dataframes.items():
        logging.info(f"Column names for {filename}: {df.columns.tolist()}")


def merge_dataframes(base_df, dataframes, base_df_name):
    merged_df = base_df
    logging.info(f"Starting with base dataframe: {base_df_name}")

    remaining_dfs = dataframes.copy()
    merged_any = True

    while remaining_dfs and merged_any:
        merged_any = False
        for filename, df in list(remaining_dfs.items()):
            common_columns = merged_df.columns.intersection(df.columns).tolist()
            if common_columns:
                logging.info(f"Merging {base_df_name} with {filename} on columns: {common_columns}")
                merged_df = pd.merge(merged_df, df, on=common_columns, how='left')
                logging.info(f"Shape after merging: {merged_df.shape}")
                remaining_dfs.pop(filename)
                merged_any = True
                gc.collect()
            else:
                logging.info(f"No common columns found between {base_df_name} and {filename}, skipping merge for now.")

    for filename, df in remaining_dfs.items():
        logging.info(f"Could not merge {filename} as there are no common columns even after all attempts.")

    return merged_df


def check_merge_integrity(base_df, dataframes, merged_df):
    logging.info("Checking merge integrity...")

    # Check row counts
    base_row_count = base_df.shape[0]
    logging.info(f"Base DataFrame row count: {base_row_count}")

    for filename, df in dataframes.items():
        row_count = df.shape[0]
        logging.info(f"{filename} row count: {row_count}")

    merged_row_count = merged_df.shape[0]
    logging.info(f"Merged DataFrame row count: {merged_row_count}")

    # Check column integrity
    base_columns = set(base_df.columns)
    merged_columns = set(merged_df.columns)
    logging.info(f"Base DataFrame columns: {base_columns}")
    logging.info(f"Merged DataFrame columns: {merged_columns}")

    missing_columns = base_columns - merged_columns
    if missing_columns:
        logging.error(f"Missing columns in merged DataFrame: {missing_columns}")
    else:
        logging.info("All columns from the base DataFrame are present in the merged DataFrame.")

    # Sample data consistency check
    sample_size = min(5, base_row_count)
    if sample_size > 0:
        sample_indices = random.sample(range(base_row_count), sample_size)
        for idx in sample_indices:
            base_row = base_df.iloc[idx]
            merged_row = merged_df.iloc[idx]
            if not base_row.equals(merged_row):
                logging.warning(f"Data mismatch at row {idx} in base DataFrame vs merged DataFrame")
            else:
                logging.info(f"Row {idx} data matches between base and merged DataFrame")


def check_unique_columns(dataframes, merged_df):
    unique_columns = set()
    for filename, df in dataframes.items():
        unique_columns.update(df.columns)
    merged_columns = set(merged_df.columns)

    missing_columns = unique_columns - merged_columns
    if missing_columns:
        logging.error(f"Missing columns in merged DataFrame: {missing_columns}")
    else:
        logging.info("All unique columns from the individual DataFrames are present in the merged DataFrame.")


def main(directory_path, base_filename, output_directory, exclude_file):
    dataframes = load_csv_files(directory_path, exclude_file)
    print_column_names(dataframes)

    base_df = dataframes.pop(base_filename)
    merged_df = merge_dataframes(base_df, dataframes, base_filename)

    output_path = os.path.join(output_directory, 'merged_data.csv')
    try:
        merged_df.to_csv(output_path, index=False)
        logging.info(f"Final merged dataframe saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving merged dataframe: {e}")

    # Check the integrity of the merge
    check_merge_integrity(base_df, dataframes, merged_df)

    # Check for missing unique columns
    check_unique_columns(dataframes, merged_df)


if __name__ == "__main__":
    input_directory = '/home/max/PycharmProjects/Hiwi_Job/BON_LTE_160524_HUE_002'
    base_filename = 'lte_seehausen.ID_L0204_V1_0_ERTRAG.csv'
    output_directory = '/home/max/Desktop/Hiwi_Job/BON_LTE_160524_HUE_002'
    exclude_file = 'lte_seehausen.ID_L020099_V1_0_PFLANZENLABORWERTE.csv'
    main(input_directory, base_filename, output_directory, exclude_file)
