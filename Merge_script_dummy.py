import os
import pandas as pd
import logging

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Prompt user for input path
    input_path = input("Please enter the input directory path: ")

    if not os.path.isdir(input_path):
        logging.error("The provided path is not a valid directory.")
        return

    # Extract directory name for output folder creation
    input_dir_name = os.path.basename(os.path.normpath(input_path))
    output_dir = os.path.join(input_path, f"{input_dir_name}_result")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{input_dir_name}_unified.csv")

    dataframes = {}

    # Load all CSV files into a dictionary of DataFrames
    for filename in os.listdir(input_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_path, filename)
            dataframes[filename] = pd.read_csv(file_path, low_memory=False)
            logging.info(f"Loaded {filename} with columns: {dataframes[filename].columns.tolist()}")
            print(f"\nDataFrame loaded from {filename}:")
            print(dataframes[filename].head())

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

    def check_dataset_quality(dataframes):
        """Perform precheck on the datasets to assess their quality and suitability for merging."""
        quality_issues = []
        overall_common_columns = set()

        for key, df in dataframes.items():
            # Check for missing values
            if df.isnull().values.any():
                logging.warning(f"{key} contains missing values.")

            # Check for rows that only contain NaNs
            if df.isna().all(axis=1).any():
                quality_issues.append(f"{key} contains rows with all NaN values.")

            # Check for columns that only contain NaNs
            if df.isna().all().any():
                quality_issues.append(f"{key} contains columns with all NaN values.")

        # Check for at least one common column across datasets
        keys = list(dataframes.keys())
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                df1, df2 = dataframes[keys[i]], dataframes[keys[j]]
                common_columns = find_common_columns(df1, df2)
                if common_columns:
                    overall_common_columns.update(common_columns)

        if not overall_common_columns:
            quality_issues.append("No common columns found across datasets.")

        if quality_issues:
            for issue in quality_issues:
                logging.warning(issue)
            return False, quality_issues
        else:
            logging.info("All datasets passed the quality check.")
            return True, []

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

            coerce_column_types(merged_df, dataframes[best_match_key], best_common_columns)
            merged_df = pd.merge(merged_df, dataframes.pop(best_match_key), on=best_common_columns, how='left')

        return merged_df

    def verify_merge(merged_df, original_dfs):
        """Verify if the merged DataFrame contains all unique columns from the original DataFrames."""
        unique_columns = set()
        for df in original_dfs.values():
            unique_columns.update(df.columns)

        missing_columns = unique_columns - set(merged_df.columns)
        if missing_columns:
            logging.warning(f"Missing columns in the final merged dataset: {missing_columns}")
        else:
            logging.info("All unique columns are present in the final merged dataset.")

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

    def check_nan_rows_columns(df):
        """Check for rows and columns that only contain NaN values."""
        # Check for rows that only contain NaNs
        nan_rows = df[df.isna().all(axis=1)]
        if not nan_rows.empty:
            print("Rows with all NaN values:")
            print(nan_rows)
        else:
            print("No rows with all NaN values found.")

        # Check for columns that only contain NaNs
        nan_columns = df.columns[df.isna().all()]
        if not nan_columns.empty:
            print("Columns with all NaN values:")
            print(nan_columns)
        else:
            print("No columns with all NaN values found.")

    # Perform the quality check
    quality_passed, issues = check_dataset_quality(dataframes)
    if quality_passed:
        # Prompt user to select the starting dataframe
        print("\nDataFrames available for merging:")
        for i, key in enumerate(dataframes.keys()):
            print(f"{i + 1}. {key}")

        start_index = int(input("\nPlease choose the starting DataFrame by entering the corresponding number: ")) - 1
        start_key = list(dataframes.keys())[start_index]

        # Start the merging process with the selected DataFrame
        merged_data = merge_dataframes(dataframes, start_key)

        # Check for rows and columns that only contain NaN values
        check_nan_rows_columns(merged_data)

        # Verify the merge
        verify_merge(merged_data, dataframes)

        # Save the final merged DataFrame to the specified output path
        merged_data.to_csv(output_file, index=False)
        logging.info(f"Final merged data saved to '{output_file}'")
    else:
        logging.error("Datasets failed the quality check. Please address the following issues:")
        for issue in issues:
            logging.error(issue)

if __name__ == "__main__":
    main()
