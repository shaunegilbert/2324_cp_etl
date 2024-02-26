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
        raise

def process_dataframe(df, processing_steps, 
                      id_column=None, 
                      rename_columns=None, 
                      new_col_name=None, 
                      new_col_value=None, 
                      concat_columns=None, 
                      occurence_col_name=None,
                      standardize_id_col=None):
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
    - occurence_col_name: str, name of the column for the counts; name in the function below
    
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
        # This checks if id_column is present, then creates a summary DataFrame
        if id_column in df.columns:
            count_df = df[id_column].value_counts().reset_index()
            count_df.columns = [id_column, occurence_col_name]  # Rename columns appropriately
            df = count_df
        else:
            logging.warning(f"{id_column} column not found in DataFrame for counting occurrences.")
            df = pd.DataFrame(columns=[id_column, 'occurrence_count'])  # Return an empty DataFrame with the expected columns if id_column is missing
    if standardize_id_col and 'standardize_id_col' in processing_steps:
        df.rename(columns=standardize_id_col, inplace=True)

    
    return df



def main ():
    try:
        base_dirs = {
            'raw': 'data/raw', 
            'processed': 'data/processed',
            'interim': 'data/interim'
        }
        
        # # Processing the first file: students.csv
        # hps_students_info = ('raw', 'students.csv')
        # hps_students_file_path = os.path.join(base_dirs[hps_students_info[0]], hps_students_info[1])
        # hps_students_columns = ['STUDENT_NUMBER', 'COHORTYR']
        # hps_students_processing_steps = ['add_constant_column', 'concatenate_id']
        
        # rename_columns = {'STUDENT_NUMBER': 'id', 'COHORTYR': 'cohort_year'}
        # new_col_name = 'district_code'
        # new_col_value = 'hps'
        # concat_columns = ['district_code', 'id']
        
        # hps_students_df = read_csv_column(hps_students_file_path, hps_students_columns)
        # hps_students_interim = process_dataframe(hps_students_df, hps_students_processing_steps,
        #                                 id_column='id',
        #                                 rename_columns=rename_columns,
        #                                 new_col_name=new_col_name,
        #                                 new_col_value=new_col_value,
        #                                 concat_columns=concat_columns)
        
        # output_file_path = os.path.join(base_dirs['interim'], 'ps_hps_students_interim.csv')
        # hps_students_interim.to_csv(output_file_path, index=False)
        # logging.info(f"Processed DataFrame has been written to {output_file_path}")
        
        # Processing the second file: jaws_students.csv
            # subset columns and write subset to data/interim/jaws_students_interim.csv
        jaws_students_info = ('raw', 'jaws_students.csv')
        jaws_students_file_path = os.path.join(base_dirs[jaws_students_info[0]], jaws_students_info[1])
        jaws_students_columns = ['Workspace Name', 
                                'gt_id', 'Subcategory', 
                                '23-24 CP Student Agreement', 
                                'Pathways 23-24']
        
        jaws_students_interim = read_csv_column(jaws_students_file_path, jaws_students_columns)
        
        jaws_students_interim_output_file_path = os.path.join(base_dirs['interim'], 'jaws_students_interim.csv')
        jaws_students_interim.to_csv(jaws_students_interim_output_file_path, index=False)
        logging.info(f"Additional processed DataFrame has been written to {jaws_students_interim_output_file_path}")
        
        # processing third file: JAWS WBL
        wbl_info = ('raw', 'jaws_wbl.csv')
        wbl_file_path = os.path.join(base_dirs[wbl_info[0]], wbl_info[1])
        wbl_columns = ['Linked field: gt_id']
        wbl_processing_steps = ['standardize_id_col', 
                                'count_occurrences']
        
        standardize_id_mapping= {'Linked field: gt_id': 'gt_id'}
        
        wbl_df = read_csv_column(wbl_file_path, wbl_columns)
        wbl_counts = process_dataframe(wbl_df, wbl_processing_steps,
                                standardize_id_col=standardize_id_mapping,
                                id_column='Linked field: gt_id',
                                occurence_col_name='WBL_count',
                                )  # Use the correct column name

        output_file_path = os.path.join(base_dirs['interim'], 'wbl_counts.csv')
        wbl_counts.to_csv(output_file_path, index=False)
        logging.info(f"Processed DataFrame has been written to {output_file_path}")

        # processed fourth file: subsetting c3 percentage and gt_id
        c3_info = ('processed', 'c3_processed.csv')
        c3_file_path = os.path.join(base_dirs[c3_info[0]], c3_info[1])
        c3_columns = ['Linked field: gt_id', 'Attendance_Percentage']
        
        c3_processing_steps=['standardize_id_col']
        
        standardize_id_mapping= {'Linked field: gt_id': 'gt_id'}
        
        c3_df = read_csv_column(c3_file_path, c3_columns)
        
        c3_df_processed = process_dataframe(c3_df, c3_processing_steps,
                                            standardize_id_col=standardize_id_mapping)
        
        output_file_path = os.path.join(base_dirs['interim'], 'c3_percentage.csv')
        c3_df_processed.to_csv(output_file_path, index=False)
        logging.info(f"Processed DataFrame has been written to {output_file_path}")
        
        # processed fifth file:
        
        
        # final output file with all fields pulled from other data sources
        merged_df = pd.merge(jaws_students_interim, wbl_counts, on='gt_id', how='left')
        
        print(merged_df)
        final_view = pd.merge(merged_df, c3_df_processed, on='gt_id', how='left')
        
        # Final merged DataFrame
        final_output_path = os.path.join(base_dirs['processed'], 'final_merged_view.csv')
        final_view.to_csv(final_output_path, index=False)
        logging.info(f"Final merged DataFrame has been written to {final_output_path}")
        
    except Exception as e:
        logging.error (f"An error occured: {e}")
        raise



if __name__ == "__main__":
    main()