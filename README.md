# Project Repository

This repository contains various scripts and tools for data processing, merging, scraping, and embedding. Below is the directory structure and a brief description of each file.

## Directory Structure
.
├── directory_structure.txt
├── grobid_client_custom.py
├── Merge & associated scripts
│   ├── BON_LTE_160524_HUE_002_merge.py
│   ├── BON_LUH_22052024_BOE_004_merge.py
│   ├── Merge_script_dummy.py
│   ├── Merge_script_experimental.py
│   └── txt_to_csv.py
└── Scraping & embedding
    ├── Embedding.py
    ├── PDF_scraping.py
    └── PDF_TEI_JSON_pipeline.py

2 directories, 10 files

# File Summaries

## Root Directory

- **directory_structure.txt**: Contains the structure of the repository.
- **grobid_client_custom.py**: A custom client for interacting with the GROBID service.

## Merge & associated scripts

- **BON_LTE_160524_HUE_002_merge.py**: Script for merging specific datasets related to BON_LTE.
- **BON_LUH_22052024_BOE_004_merge.py**: Script for merging specific datasets related to BON_LUH.
- **Merge_script_dummy.py**: A dummy merge script for testing purposes.
- **Merge_script_experimental.py**: An experimental merge script for new merging techniques.
- **txt_to_csv.py**: Script to convert text files to CSV format.


## Scraping & embedding

- **Embedding.py**: Script for embedding data.
- **PDF_scraping.py**: Script for scraping data from PDF files.
- **PDF_TEI_JSON_pipeline.py**: Pipeline script for converting PDF to TEI and then to JSON format.
- **Reference_paper**: https://arxiv.org/pdf/2401.08406


# DataFrame Merger (Merge & associated scripts/Merge_script_dummy.py)

## Overview

This script is designed to load, preprocess, and merge multiple CSV files located in a specified directory. It includes functionality to:
- Load CSV files into pandas DataFrames
- Normalize column names
- Identify and address quality issues such as missing values and NaN-only rows or columns
- Find common columns across DataFrames for merging
- Coerce column types for consistency during merging
- Rank DataFrames based on the number of common columns
- Merge DataFrames iteratively
- Verify the final merged DataFrame

## Features

1. **Load CSV Files**: Load all CSV files from a specified directory into pandas DataFrames.
2. **Column Normalization**: Normalize column names to uppercase without leading/trailing whitespace.
3. **Quality Check**: Perform prechecks on datasets to identify missing values and NaN-only rows/columns.
4. **Common Columns Identification**: Identify common columns between pairs of DataFrames and across all DataFrames.
5. **Column Type Coercion**: Coerce column types to ensure consistency during merging.
6. **DataFrame Ranking**: Rank DataFrames based on the number of common columns with other DataFrames.
7. **Merging**: Merge all DataFrames iteratively based on the most common columns.
8. **Merge Verification**: Verify the integrity of the merged DataFrame.

## Usage

1. **Setup**:
   - Ensure you have Python installed.
   - Install the required packages:
     ```bash
     pip install pandas
     ```

2. **Run the Script**:
   - Execute the script from the command line:
     ```bash
     python script_name.py
     ```

3. **User Prompts**:
   - **Input Directory**:
     - When prompted, enter the path to the directory containing the CSV files:
       ```
       Please enter the input directory path: 
       ```
     - Example response:
       ```
       /path/to/csv/files
       ```

   - **Quality Check Decision**:
     - If quality issues are found (columns or rows with all NaN values), you will be asked whether to delete these entries:
       ```
       The following columns and rows contain only NaN values:
       Column: [column_name]
       Rows in [dataframe_name]: [row_indices]

       Would you like to delete these columns and rows and reload the data? (yes/no): 
       ```
     - Example response:
       ```
       yes
       ```

   - **Merge Column Choice**:
     - After identifying common columns, you will be prompted to decide whether to merge based on these columns:
       ```
       Common columns found across all DataFrames: [common_columns]
       Would you like to merge all DataFrames based on one of these common columns? (yes/no): 
       ```
     - Example response:
       ```
       yes
       ```

     - If you choose to merge based on common columns, you will be prompted to select a specific column:
       ```
       Common columns available for merging:
       1. column_name1
       2. column_name2
       ...
       Please choose the common column by entering the corresponding number: 
       ```
     - Example response:
       ```
       1
       ```

   - **Starting DataFrame Choice**:
     - You will be prompted to select the starting DataFrame for the merge process:
       ```
       DataFrames available for merging (ranked by common columns with others):
       1. dataframe_name1 (common columns: count)
       2. dataframe_name2 (common columns: count)
       ...
       Please choose the starting DataFrame by entering the corresponding number: 
       ```
     - Example response:
       ```
       1
       ```

4. **Output**:
   - The merged DataFrame is saved in a new subdirectory within the input directory.
   - The output file will be named `[input_directory_name]_unified.csv`.

## Functions

### `main()`
The main function that orchestrates the entire process from loading data to saving the merged DataFrame.

### `load_dataframes(directory)`
Loads all CSV files in the specified directory into pandas DataFrames.

### `find_common_columns(df1, df2)`
Finds common columns between two DataFrames.

### `find_common_columns_across_all(dataframes)`
Finds common columns across all DataFrames.

### `coerce_column_types(df1, df2, columns)`
Coerces column types to ensure consistency for merging.

### `check_dataset_quality(dataframes)`
Performs a quality check on the datasets to assess their quality and suitability for merging.

### `rank_dataframes_by_common_columns(dataframes)`
Ranks DataFrames based on the number of common columns with other DataFrames.

### `merge_dataframes(dataframes, start_key, merge_columns=None)`
Merges all DataFrames iteratively based on the most common columns.

### `verify_merge(merged_df, original_dfs)`
Verifies if the merged DataFrame contains all unique columns from the original DataFrames.

### `check_nan_rows_columns(df)`
Checks for rows and columns that only contain NaN values, excluding the first row.
