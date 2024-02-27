import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv_column(file_path, columns):
    """
    Reads specific columns from a CSV file into a DataFrame.
    """
    try:
        df = pd.read_csv(file_path, usecols=columns)
        logging.info(f"Successfully read {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        raise

def rename_columns(df, rename_map):
    """
    Renames columns in the DataFrame according to a provided mapping.
    """
    try:
        df.rename(columns=rename_map, inplace=True)
        logging.info("Columns renamed successfully.")
    except Exception as e:
        logging.error(f"Error renaming columns: {e}")
        raise
    return df

def count_occurrences(df, id_column, occurence_col_name):
    """
    Counts occurrences of unique values in the specified ID column.
    """
    try:
        count_df = df[id_column].value_counts().reset_index()
        count_df.columns = [id_column, occurence_col_name]
        logging.info(f"Occurrences counted successfully for {id_column}.")
    except Exception as e:
        logging.error(f"Error counting occurrences for {id_column}: {e}")
        raise
    return count_df

def standardize_id_col(df, standardize_map):
    """
    Standardizes ID column names according to a provided mapping.
    """
    try:
        df.rename(columns=standardize_map, inplace=True)
        logging.info("ID column standardized successfully.")
    except Exception as e:
        logging.error(f"Error standardizing ID column: {e}")
        raise
    return df

def process_jaws_students(base_dirs):
    """
    Processes the jaws_students.csv file.
    """
    file_path = os.path.join(base_dirs['raw'], 'jaws_students.csv')
    columns = ['Workspace Name', 'gt_id', 'Subcategory', '23-24 CP Student Agreement', 'Pathways 23-24']
    df = read_csv_column(file_path, columns)
    output_file_path = os.path.join(base_dirs['interim'], 'jaws_students_interim.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"Jaws students DataFrame has been written to {output_file_path}")
    return df

def process_wbl(base_dirs):
    """
    Processes the jaws_wbl.csv file.
    """
    file_path = os.path.join(base_dirs['raw'], 'jaws_wbl.csv')
    columns = ['Linked field: gt_id']
    standardize_map = {'Linked field: gt_id': 'gt_id'}
    df = read_csv_column(file_path, columns)
    df = standardize_id_col(df, standardize_map)
    df = count_occurrences(df, 'gt_id', 'WBL_count')
    output_file_path = os.path.join(base_dirs['interim'], 'wbl_counts.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"WBL DataFrame has been written to {output_file_path}")
    return df

def process_c3(base_dirs):
    """
    Processes the c3_percentage.csv file.
    """
    file_path = os.path.join(base_dirs['processed'], 'c3_processed.csv')
    columns = ['Linked field: gt_id', 'Attendance_Percentage']
    standardize_map = {'Linked field: gt_id': 'gt_id'}
    df = read_csv_column(file_path, columns)
    df = standardize_id_col(df, standardize_map)
    output_file_path = os.path.join(base_dirs['interim'], 'c3_percentage.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"C3 DataFrame has been written to {output_file_path}")
    return df

def process_check_in(base_dirs):
    """
    Processes the check_in.csv file.
    """
    file_path = os.path.join(base_dirs['raw'], 'check_in.csv')
    columns = ['Linked field: gt_id']
    standardize_map = {'Linked field: gt_id': 'gt_id'}
    df = read_csv_column(file_path, columns)
    df = standardize_id_col(df, standardize_map)
    df = count_occurrences(df, 'gt_id', 'Check_in_count')
    output_file_path = os.path.join(base_dirs['interim'], 'check_in_counts.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"Check-in DataFrame has been written to {output_file_path}")
    return df

def process_cert(base_dirs):
    '''
    Process the cert_interim.csv
    '''
    file_path = os.path.join(base_dirs['raw'], 'cert.csv')
    columns = ['Linked field: gt_id']
    standardize_map = {'Linked field: gt_id': 'gt_id'}
    df = read_csv_column(file_path, columns)
    df = standardize_id_col(df, standardize_map)
    df = count_occurrences(df, 'gt_id', 'cert_count')
    output_file_path = os.path.join(base_dirs['interim'], 'cert_counts.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"Check-in DataFrame has been written to {output_file_path}")
    return df

def merge_dataframes(base_dirs, jaws_students_df, wbl_df, c3_df, cert_df, check_in_df):
    """
    Merges all processed DataFrames and writes the final merged DataFrame to csv.
    """
    merged_df = pd.merge(jaws_students_df, wbl_df, on='gt_id', how='left')
    merged_df = pd.merge(merged_df, check_in_df, on='gt_id', how='left')
    merged_df = pd.merge(merged_df, cert_df, on='gt_id', how='left')
    final_view = pd.merge(merged_df, c3_df, on='gt_id', how='left')
    final_output_path = os.path.join(base_dirs['processed'], 'final_merged_view.csv')
    final_view.to_csv(final_output_path, index=False)
    logging.info(f"Final merged DataFrame has been written to {final_output_path}")

def main():
    try:
        base_dirs = {
            'raw': 'data/raw',
            'processed': 'data/processed',
            'interim': 'data/interim'
        }

        # Process individual datasets
        jaws_students_df = process_jaws_students(base_dirs)
        wbl_df = process_wbl(base_dirs)
        c3_df = process_c3(base_dirs)
        check_in_df = process_check_in(base_dirs)
        cert_df=process_cert(base_dirs)

        # Merge all processed DataFrames
        merge_dataframes(base_dirs, jaws_students_df, wbl_df, c3_df, cert_df, check_in_df)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()