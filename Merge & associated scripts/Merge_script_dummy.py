import os
import pandas as pd
import logging
from collections import defaultdict

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    def load_dataframes(directory):
        dataframes = {}
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(directory, filename)
                try:
                    df = pd.read_csv(file_path, low_memory=False)
                    df.columns = [col.strip().upper() for col in df.columns]  # Normalize column names
                    dataframes[filename] = df
                    logging.info(f"Loaded {filename} with columns: {df.columns.tolist()}")
                    print(f"\nDataFrame loaded from {filename}:")
                    print(df.head())
                except Exception as e:
                    logging.error(f"Failed to load {filename}: {e}")
        return dataframes

    def find_common_columns(df1, df2):
        """Find common columns between two DataFrames."""
        return list(set(df1.columns).intersection(set(df2.columns)))

    def find_common_columns_across_all(dataframes):
        """Find common columns across all DataFrames."""
        common_columns = set(dataframes[next(iter(dataframes))].columns)
        for df in dataframes.values():
            common_columns.intersection_update(df.columns)
            if not common_columns:
                break
        return list(common_columns)

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
        columns_to_remove = set()
        rows_to_remove = defaultdict(list)

        for key, df in dataframes.items():
            # Check for missing values
            if df.isnull().values.any():
                logging.warning(f"{key} contains missing values.")

            # Check for rows that only contain NaNs (excluding the first row)
            nan_rows = df.iloc[1:].index[df.iloc[1:].isna().all(axis=1)]
            if not nan_rows.empty:
                quality_issues.append(f"{key} contains rows with all NaN values: {nan_rows.tolist()}")
                rows_to_remove[key].extend(nan_rows.tolist())

            # Check for columns that only contain NaNs (excluding the first row)
            nan_columns = df.columns[df.iloc[1:].isna().all()]
            if not nan_columns.empty:
                columns_to_remove.update(nan_columns)
                quality_issues.append(f"{key} contains columns with all NaN values: {nan_columns.tolist()}")

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
            return False, quality_issues, columns_to_remove, rows_to_remove
        else:
            logging.info("All datasets passed the quality check.")
            return True, [], columns_to_remove, rows_to_remove

    def rank_dataframes_by_common_columns(dataframes):
        """Rank dataframes based on the number of common columns with other dataframes."""
        common_columns_count = defaultdict(int)
        keys = list(dataframes.keys())

        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                df1, df2 = dataframes[keys[i]], dataframes[keys[j]]
                common_columns = find_common_columns(df1, df2)
                common_columns_count[keys[i]] += len(common_columns)
                common_columns_count[keys[j]] += len(common_columns)

        ranked_dataframes = sorted(common_columns_count.items(), key=lambda item: item[1], reverse=True)
        return ranked_dataframes

    def merge_dataframes(dataframes, start_key, merge_columns=None):
        """Merge all DataFrames iteratively based on the most common columns."""
        merged_df = dataframes.pop(start_key)
        logging.info(f"Starting merge with {start_key}")

        while dataframes:
            best_match_key = None
            best_common_columns = merge_columns if merge_columns else []
            for key, df in dataframes.items():
                common_columns = find_common_columns(merged_df, df)
                if merge_columns:
                    common_columns = merge_columns
                if len(common_columns) > len(best_common_columns):
                    best_match_key = key
                    best_common_columns = common_columns
            if not best_common_columns:
                raise ValueError("No common columns found for merging.")

            logging.info(f"Merging with {best_match_key} on columns: {best_common_columns}")

            # Coerce column types
            coerce_column_types(merged_df, dataframes[best_match_key], best_common_columns)

            # Perform the merge
            try:
                merged_df = pd.merge(merged_df, dataframes.pop(best_match_key), on=best_common_columns, how='outer')
            except MemoryError as e:
                logging.error(f"MemoryError during merge: {e}")
                return None

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
        """Check for rows and columns that only contain NaN values, excluding the first row."""
        # Check for rows that only contain NaNs, excluding the first row
        nan_rows = df.iloc[1:][df.iloc[1:].isna().all(axis=1)]
        if not nan_rows.empty:
            print("Rows with all NaN values (excluding the first row):")
            print(nan_rows)
        else:
            print("No rows with all NaN values found (excluding the first row).")

        # Check for columns that only contain NaNs, excluding the first row
        nan_columns = df.columns[df.iloc[1:].isna().all()]
        if not nan_columns.empty:
            print("Columns with all NaN values (excluding the first row):")
            print(nan_columns)
        else:
            print("No columns with all NaN values found (excluding the first row).")

    input_path = input("Please enter the input directory path: ")

    if not os.path.isdir(input_path):
        logging.error("The provided path is not a valid directory.")
        return

    # Extract directory name for output folder creation
    input_dir_name = os.path.basename(os.path.normpath(input_path))
    output_dir = os.path.join(input_path, f"{input_dir_name}_result")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{input_dir_name}_unified.csv")

    dataframes = load_dataframes(input_path)

    # Perform the quality check
    quality_passed, issues, columns_to_remove, rows_to_remove = check_dataset_quality(dataframes)
    if not quality_passed:
        # Print information about columns and rows with all NaN values in each dataframe
        if columns_to_remove or rows_to_remove:
            print("The following columns and rows contain only NaN values:")
            for column in columns_to_remove:
                print(f"Column: {column}")
            for df_name, rows in rows_to_remove.items():
                print(f"Rows in {df_name}: {rows}")

            # Prompt user to decide whether to delete columns and rows with all NaN values
            remove_nan_entries = input("Would you like to delete these columns and rows and reload the data? (yes/no): ").strip().lower()
            if remove_nan_entries == 'yes':
                # Remove columns and rows with all NaN values and reload dataframes
                for df_name, df in dataframes.items():
                    df.drop(columns=[col for col in columns_to_remove if col in df.columns], inplace=True)
                    df.drop(index=[row for row in rows_to_remove[df_name] if row in df.index], inplace=True)
                # Save the cleaned dataframes
                for df_name, df in dataframes.items():
                    df.to_csv(os.path.join(input_path, df_name), index=False)
                # Reload dataframes
                dataframes = load_dataframes(input_path)
                quality_passed, issues, columns_to_remove, rows_to_remove = check_dataset_quality(dataframes)

    if quality_passed:
        # Check for common columns across all DataFrames
        common_columns = find_common_columns_across_all(dataframes)
        if common_columns:
            print(f"Common columns found across all DataFrames: {common_columns}")
            merge_column_choice = input("Would you like to merge all DataFrames based on one of these common columns? (yes/no): ").strip().lower()
            if merge_column_choice == 'yes':
                print("\nCommon columns available for merging:")
                for i, col in enumerate(common_columns, 1):
                    print(f"{i}. {col}")
                column_index = int(input("\nPlease choose the common column by entering the corresponding number: ")) - 1
                merge_column = common_columns[column_index]
            else:
                merge_column = None
        else:
            merge_column = None

        # Rank dataframes by the number of common columns with other dataframes
        ranked_dataframes = rank_dataframes_by_common_columns(dataframes)

        # Prompt user to select the starting dataframe
        print("\nDataFrames available for merging (ranked by common columns with others):")
        for i, (key, count) in enumerate(ranked_dataframes):
            print(f"{i + 1}. {key} (common columns: {count})")

        start_index = int(input("\nPlease choose the starting DataFrame by entering the corresponding number: ")) - 1
        start_key = ranked_dataframes[start_index][0]

        # Start the merging process with the selected DataFrame
        merged_data = merge_dataframes(dataframes, start_key, merge_columns=[merge_column] if merge_column else None)

        if merged_data is None:
            logging.error("Merging failed due to memory error.")
            return

        # Check for rows and columns that only contain NaN values, excluding the first row
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
