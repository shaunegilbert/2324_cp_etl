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
    df=read_csv_column(file_path, columns)

    # Add a new column 'currently_enrolled' with value 'enrolled'
    df['status'] = 'enrolled'

    output_file_path = os.path.join(base_dirs['interim'], 'current_courses_interim.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"WBL DataFrame has been written to {output_file_path}")
    return df

def pull_completed_courses (base_dirs):
    file_path = os.path.join(base_dirs['raw'], 'stored_grades.csv')
    columns=['STUDENT_NUMBER',
             'COURSE_NUMBER',
             'EARNEDCRHRS']
    df=read_csv_column(file_path, columns)

    # Add a new column 'status' based on the condition: if EARNEDCRHRS == 1 then 'passed', else 'na'
    df['status'] = df['EARNEDCRHRS'].apply(lambda x: 'passed' if x == 1 else 'no credit earned')

    # Drop the 'EARNEDCRHRS' column
    df.drop(columns=['EARNEDCRHRS'], inplace=True)

    output_file_path = os.path.join(base_dirs['interim'], 'completed_courses_interim.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"WBL DataFrame has been written to {output_file_path}")
    return df

def final_course_df (base_dirs):
    df_current = pull_current_courses(base_dirs)
    df_completed = pull_completed_courses(base_dirs)
    df_codes=pull_course_codes(base_dirs)
    students=pull_students_sub(base_dirs)

    # Merge the two DataFrames vertically
    df_merged = pd.concat([df_current, df_completed])
    
    # Merge df_merged with df_codes on 'COURSE_NUMBER' and 'course_code_l'
    df_merged = pd.merge(df_merged, df_codes, how='left', left_on='COURSE_NUMBER', right_on='course_code_l')

    #drop course code columns
    # df_merged.drop(columns=['COURSE_NUMBER',
    #                  'course_code_l'], inplace=True)
    
     # Pivot the DataFrame
    # df_pivot = df_merged.pivot_table(index='STUDENT_NUMBER', columns='course_name_l', values='status', aggfunc='first')

    df_merged = pd.merge(students, df_merged, how='right', on='STUDENT_NUMBER')

    output_file_path = os.path.join(base_dirs['interim'], 'final_course.csv')
    df_merged.to_csv(output_file_path, index=False)
    logging.info(f"Merged DataFrame has been written to {output_file_path}")
    return df_merged

def main():
    try:
        base_dirs = {
            'raw': 'data/raw',
            'processed': 'data/processed',
            'interim': 'data/interim'}

        #process individual datasets
        pull_students_sub (base_dirs)
        pull_course_codes (base_dirs)
        pull_current_courses (base_dirs)
        pull_completed_courses(base_dirs)
        final_course_df(base_dirs)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()