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
               'course_name_l',
               'pathway_code']
    df = read_csv_column(file_path, columns)

    output_file_path = os.path.join(base_dirs['interim'], 'course_codes.csv')
    df.to_csv(output_file_path, index=False)
    logging.info(f"WBL DataFrame has been written to {output_file_path}")
    return df
    
def pull_current_courses (base_dirs):
    file_path = os.path.join(base_dirs['raw'], 'current_courses.csv')
    columns = ['STUDENT_NUMBER',
               'COURSE_NUMBER',
               'TERMID']
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
    courses_merged = pd.concat([df_current, df_completed])
    
    # Merge courses_merged with df_codes on 'COURSE_NUMBER' and 'course_code_l'
    courses_merged = pd.merge(courses_merged, df_codes, how='left', left_on='COURSE_NUMBER', right_on='course_code_l')

    #drop course code columns
    # courses_merged.drop(columns=['COURSE_NUMBER',
    #                  'course_code_l'], inplace=True)
    
     # Pivot the DataFrame
    # df_pivot = courses_merged.pivot_table(index='STUDENT_NUMBER', columns='course_name_l', values='status', aggfunc='first')

    courses_merged = pd.merge(students, courses_merged, how='right', on='STUDENT_NUMBER')

    # Filter rows based on column: 'SCHOOL_NAME' -- drop records where school is NaN
    courses_merged = courses_merged[courses_merged['SCHOOL_NAME'].notna()]

    # filter rows based on col 'TERMID' -- drop records wehere TERMID does not start with 33
    courses_merged = courses_merged[courses_merged['TERMID'].astype(str).str.startswith('33')]

    # filter rows based on column 'EXITDATE' -- drop any records with exit dates
    courses_merged = courses_merged[courses_merged['EXITDATE'].isna()]

    #change student number data type to string
    courses_merged = courses_merged.astype({'STUDENT_NUMBER': 'string'})

    output_file_path = os.path.join(base_dirs['interim'], 'final_course.csv')
    courses_merged.to_csv(output_file_path, index=False)
    logging.info(f"Merged DataFrame has been written to {output_file_path}")
    return courses_merged

# Custom function to determine pathway
import pandas as pd

def determine_pathway(row):
    if row['SCHOOL_NAME'] == 'Hartford Public High School':
        if row['STEM'] == 'Yes' and pd.isna(row['HC']):
            return 'STEM'
        elif pd.isna(row['STEM']) and row['HC'] == 'Yes':
            return 'HC'
        elif row['STEM'] == 'Yes' and row['HC'] == 'Yes':
            return 'Dual'
        else:
            return 'No Pathway'
    elif row['SCHOOL_NAME'] == 'Weaver High School':
        if row['IF'] == 'Yes' and pd.isna(row['JM']):
            return 'IF'
        elif pd.isna(row['IF']) and row['JM'] == 'Yes':
            return 'JM'
        elif row['IF'] == 'Yes' and row['JM'] == 'Yes':
            return 'Dual'
        else:
            return 'No Pathway'
    elif row['SCHOOL_NAME'] == 'Pathways Academy of Technology and Design':
        if row['STEM'] == 'Yes':
            return 'STEM'
        else:
            return 'No Pathway'
    elif row['SCHOOL_NAME'] == 'Bulkeley High School':
        if row['STEM'] == 'Yes' and pd.isna(row['PS']):
            return 'STEM'
        elif pd.isna(row['STEM']) and row['PS'] == 'Yes':
            return 'PS'
        elif row['STEM'] == 'Yes' and row['PS'] == 'Yes':
            return 'Dual'
        else:
            return 'No Pathway'
    else:
        return 'No Pathway'  # Default pathway for any other school not explicitly handled



def pathway_code_pivot (base_dirs):
    courses_merged = final_course_df(base_dirs)
    students = pull_students_sub(base_dirs)

    # Preparing for pivot: Creating a column to indicate presence of a pathway_code
    courses_merged['pathway_present'] = 'Yes'

    # Step 3: Pivot 'pathway_code' to create new columns
    df_pivot = courses_merged.pivot_table(index='STUDENT_NUMBER', columns='pathway_code', values='pathway_present', aggfunc='first')

    # If they differ, convert them to the same type, typically string if there's inconsistency:
    students['STUDENT_NUMBER'] = students['STUDENT_NUMBER'].astype(str)
    df_pivot = df_pivot.reset_index()  # Reset index if STUDENT_NUMBER is the index
    df_pivot['STUDENT_NUMBER'] = df_pivot['STUDENT_NUMBER'].astype(str)

    # Merging the pivoted data back with the student's essential information
    pathway_code_pivot = pd.merge(students, df_pivot, how='left', on='STUDENT_NUMBER')

    pathway_code_pivot['pathway'] = pathway_code_pivot.apply(determine_pathway, axis=1)

    print(courses_merged['STUDENT_NUMBER'].dtype, students['STUDENT_NUMBER'].dtype)

    output_file_path = os.path.join(base_dirs['interim'], 'final_pathway_code_pivot.csv')
    pathway_code_pivot.to_csv(output_file_path, index=True)
    logging.info(f"Merged DataFrame has been written to {output_file_path}")
    return pathway_code_pivot

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
        pathway_code_pivot(base_dirs)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()