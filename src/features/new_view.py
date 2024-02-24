import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv_column(file_path, columns):
    """
    Reads specific columns from a CSV file into a DataFrame.
    
    Parameters:
    - file_path: str, the path to the CSV file.
    - columns: list, a list of column names to read from the file.
    
    Returns:
    - A pandas DataFrame containing the specified columns.
    """
    try:
        df = pd.read_csv(file_path, usecols=columns)
        df.fillna
        logging.info(f"Successfully read {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def process_dataframe(df, processing_steps, id_column=None, additional_data=None, rename_columns=None, new_col_name=None, new_col_value=None, concat_columns=None):
    """
    Processes the DataFrame for necessary transformations or calculations.
    
    Parameters:
    - df: DataFrame, the DataFrame to process.
    - processing_steps: list, a list of processing steps to apply.
    - id_column: str, the name of the unique ID column (optional).
    - additional_data: DataFrame, additional data needed for certain calculations (optional).
    - rename_columns: dict, a mapping of old column names to new column names (optional).
    - new_col_name: str, the name of the new column to add with a constant value (optional).
    - new_col_value: str, the constant value for the new column (optional).
    - concat_columns: list, the columns to concatenate to create a new ID (optional).
    
    Returns:
    - The processed DataFrame.
    """
    if rename_columns:
        df.rename(columns=rename_columns, inplace=True)
    
    if 'add_constant_column' in processing_steps and new_col_name and new_col_value is not None:
        df[new_col_name] = new_col_value
    
    if 'concatenate_id' in processing_steps and concat_columns:
        df['gt_id'] = df[concat_columns].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
    
    if 'count_occurrences' in processing_steps and id_column:
        df['event_count'] = df.groupby(id_column)[id_column].transform('count')
        
    
    
    # Include other processing steps as necessary
    
    return df


# def merge_dataframes(df1, df2, on, how='inner'):
#     """
#     Merges two DataFrames based on a unique ID column.
    
#     Parameters:
#     - df1: DataFrame, the first DataFrame to merge.
#     - df2: DataFrame, the second DataFrame to merge.
#     - on: str, the name of the column to merge on (unique ID).
#     - how: str, type of merge to perform (default 'inner').
    
#     Returns:
#     - A merged pandas DataFrame.
#     """
#     try:
#         merged_df = pd.merge(df1, df2, on=on, how=how)
#         logging.info("DataFrames merged successfully")
#         return merged_df
#     except Exception as e:
#         logging.error(f"Error merging dataframes: {e}")
#         return pd.DataFrame()

# def aggregate_data(file_paths, columns, processing_steps):
#     """
#     Aggregates data from multiple CSV files into a single DataFrame, processing each
#     DataFrame before merging.
    
#     Parameters:
#     - file_paths: list, paths to the CSV files.
#     - columns: dict, mapping of file paths to lists of columns to read.
#     - processing_steps: dict, processing steps for each file.
    
#     Returns:
#     - A pandas DataFrame with combined data from all files after individual processing.
#     """
#     merged_df = None
#     for file_path in file_paths:
#         df = read_csv_column(file_path, columns[file_path])
#         processed_df = process_dataframe(df, processing_steps[file_path], 'unique_id')  # Adjust processing as needed
#         if merged_df is None:
#             merged_df = processed_df
#         else:
#             merged_df = merge_dataframes(merged_df, processed_df, on='unique_id')
#     return merged_df


import logging
import os

# Assuming the definition of read_csv_column and process_dataframe functions are above

if __name__ == "__main__":
    base_dirs = {
        'raw': 'data/raw', 
        'processed': 'data/processed',
        'interim': 'data/interim'
    }
    
    # Processing the first file: students.csv
    hps_students_info = ('raw', 'students.csv')
    hps_students_file_path = os.path.join(base_dirs[hps_students_info[0]], hps_students_info[1])
    hps_students_columns = ['STUDENT_NUMBER', 'COHORTYR']
    hps_students_processing_steps = ['add_constant_column', 'concatenate_id']
    
    rename_columns = {'STUDENT_NUMBER': 'id', 'COHORTYR': 'cohort_year'}
    new_col_name = 'district_code'
    new_col_value = 'hps'
    concat_columns = ['district_code', 'id']
    
    df = read_csv_column(hps_students_file_path, hps_students_columns)
    processed_df = process_dataframe(df, hps_students_processing_steps,
                                     id_column='id',
                                     rename_columns=rename_columns,
                                     new_col_name=new_col_name,
                                     new_col_value=new_col_value,
                                     concat_columns=concat_columns)
    
    output_file_path = os.path.join(base_dirs['interim'], 'ps_hps_students_interim.csv')
    processed_df.to_csv(output_file_path, index=False)
    logging.info(f"Processed DataFrame has been written to {output_file_path}")
    
    ##########
    
    # Processing the second file: jaws_students.csv
        # subset columns and write subset to data/interim/jaws_students_interim.csv
    jaws_students_info = ('raw', 'jaws_students.csv')
    jaws_students_file_path = os.path.join(base_dirs[jaws_students_info[0]], jaws_students_info[1])
    jaws_students_columns = ['Workspace Name', 'gt_id', 'Subcategory', '23-24 CP Student Agreement', 'Pathways 23-24']
    
    jaws_students_interim = read_csv_column(jaws_students_file_path, jaws_students_columns)
    
    jaws_students_interim_output_file_path = os.path.join(base_dirs['interim'], 'jaws_students_interim.csv')
    jaws_students_interim.to_csv(jaws_students_interim_output_file_path, index=False)
    logging.info(f"Additional processed DataFrame has been written to {jaws_students_interim_output_file_path}")
    
    # processing third file: 

