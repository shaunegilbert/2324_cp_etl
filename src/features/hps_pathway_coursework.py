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

def subset_dataframe(df, column, values):
    """
    Subsets DataFrame based on values in a specified column.
    """
    try:
        subset_df = df[df[column].isin(values)]
        return subset_df
    except Exception as e:
        logging.error(f"Error subsetting DataFrame: {e}")
        raise

def pull_students_sub (base_dirs):
    file_path = os.path.join(base_dirs['raw'], 'students.csv')
    columns = ['STUDENT_NUMBER', 
               'LAST_NAME', 
               'FIRST_NAME', 
               'COHORTYR', 
               'GRADE_LEVEL', 
               'SCHOOL_NAME', 
               'EXITDATE']
    df = read_csv_column(file_path, columns)

    # subset_df = subset_dataframe(df, 'SCHOOL_NAME', ['Hartford Public High School'])

    output_file_path = os.path.join(base_dirs['interim'], 'hps_students_for_coursework.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"WBL DataFrame has been written to {output_file_path}")
    return df

def pull_course_codes (base_dirs):
    file_path= os.path.join(base_dirs['raw'], 'course_codes.csv')
    columns = ['course_code_l',
               'course_name_l']
    df = read_csv_column(file_path, columns)

    output_file_path = os.path.join(base_dirs['interim'], 'course_codes.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"WBL DataFrame has been written to {output_file_path}")
    return df
    
def pull_current_courses (base_dirs):
    file_path = os.path.join(base_dirs['raw'], 'current_courses.csv')
    columns = ['STUDENT_NUMBER',
               'COURSE_NUMBER']


def main():
    try:
        base_dirs = {
            'raw': 'data/raw',
            'processed': 'data/processed',
            'interim': 'data/interim'
        }

        #process individual datasets
        pull_students_sub (base_dirs)
        pull_course_codes (base_dirs)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()