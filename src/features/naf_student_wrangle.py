import os
import pandas as pd
import logging
from typing import Dict, List


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_csv_column(file_path: str, columns: List[str], dtype: Dict[str, str] = None) -> pd.DataFrame:
    """
    Reads specific columns from a CSV file into a DataFrame.

    Args:
        file_path (str): The path to the CSV file.
        columns (List[str]): The list of column names to read.
        dtype (Dict[str, str], optional): A dictionary mapping column names to data types.

    Returns:
        pd.DataFrame: The DataFrame containing the specified columns.
    """
    try:
        df = pd.read_csv(file_path, usecols=columns, dtype=dtype)
        logging.info(f"Successfully read {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        raise


def save_dataframe_to_csv(df: pd.DataFrame, file_path: str):
    """
    Saves a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        file_path (str): The path to save the CSV file.
    """
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"DataFrame has been written to {file_path}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {file_path}: {e}")
        raise


def pull_students_sub(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls student data from a CSV file and saves it to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The student DataFrame.
    """
    file_path = os.path.join(base_dirs['raw'], 'students.csv')
    columns = ['STUDENT_NUMBER', 'LAST_NAME', 'FIRST_NAME', 'COHORTYR', 'GRADE_LEVEL', 'SCHOOL_NAME', 'HOUSE', 'EXITDATE']
    df = read_csv_column(file_path, columns, dtype={"STUDENT_NUMBER": "string"})
    df = df.drop_duplicates(keep='first')
    output_file_path = os.path.join(base_dirs['interim'], 'hps_students_for_coursework.csv')
    save_dataframe_to_csv(df, output_file_path)
    return df

def pull_student_agreement_sub(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls jaws_students_report from a CSV file and saves it to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: jaws_students DataFrame.
    """
    file_path = os.path.join(base_dirs['raw'], 'student_agreement.csv')
    columns = ['Student ID Number', 'district code', 'Please select your Career Pathway' ]
    student_agreement = read_csv_column(file_path, columns, dtype={"Student ID Number": "string"})
    return student_agreement


def pull_course_codes(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls course codes from a CSV file and saves it to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The course codes DataFrame.
    """
    file_path = os.path.join(base_dirs['raw'], 'course_codes.csv')
    columns = ['course_code_l', 'course_name_l', 'pathway_code']
    df = read_csv_column(file_path, columns)
    output_file_path = os.path.join(base_dirs['interim'], 'course_codes.csv')
    save_dataframe_to_csv(df, output_file_path)
    return df


def pull_current_courses(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls current courses from a CSV file, filters them, and saves to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The current courses DataFrame.
    """
    file_path = os.path.join(base_dirs['raw'], 'current_courses.csv')
    columns = ['STUDENT_NUMBER', 'COURSE_NUMBER', 'TERMID']
    df = read_csv_column(file_path, columns, dtype={"STUDENT_NUMBER": "string"})

    df = df[df['TERMID'].astype(str).str.startswith('33')]
    df['status'] = 'enrolled'

    output_file_path = os.path.join(base_dirs['interim'], 'current_courses_interim.csv')
    save_dataframe_to_csv(df, output_file_path)
    return df


def pull_completed_courses(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls completed courses from a CSV file, filters them, and saves to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The completed courses DataFrame.
    """
    file_path = os.path.join(base_dirs['raw'], 'stored_grades.csv')
    columns = ['STUDENT_NUMBER', 'COURSE_NUMBER', 'EARNEDCRHRS']
    df = read_csv_column(file_path, columns, dtype={"STUDENT_NUMBER": "string"})

    df['status'] = df['EARNEDCRHRS'].apply(lambda x: 'passed' if x == 1 else 'no credit earned')
    df.drop(columns=['EARNEDCRHRS'], inplace=True)

    output_file_path = os.path.join(base_dirs['interim'], 'completed_courses_interim.csv')
    save_dataframe_to_csv(df, output_file_path)
    return df

def pull_wbl_counts(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls wbl counts from a CSV file, filters them, and saves to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The wbl counts DataFrame.
    """
    file_path = os.path.join(base_dirs['interim'], 'wbl_counts.csv')
    columns = ['gt_id', 'WBL_count']
    wbl_counts = read_csv_column(file_path, columns)
    return wbl_counts

def pull_internship_counts(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls internship counts from a CSV file, filters them, and saves to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The internship counts DataFrame.
    """
    file_path = os.path.join(base_dirs['interim'], 'internships_count.csv')
    columns = ['gt_id', 'internship_count']
    internship_counts = read_csv_column(file_path, columns)
    return internship_counts


def pull_naf_students(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Pulls naf_students from a CSV file, filters them, and saves to an interim file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The naf_students DataFrame.
    """
    file_path = os.path.join(base_dirs['raw'], 'naf_students.csv')
    columns = ['Student ID', 'First Name', 'Last Name', 'Academies', 'NTC Progress', 'Is NAF', 'Graduated', 'Active']
    df = read_csv_column(file_path, columns, dtype={"STUDENT_NUMBER": "string"})

    return df


def final_course_df(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Generates the final course DataFrame by merging various DataFrames and saves to a final file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The final merged DataFrame.
    """
    df_current = pull_current_courses(base_dirs)
    df_completed = pull_completed_courses(base_dirs)
    df_codes = pull_course_codes(base_dirs)
    df_students = pull_students_sub(base_dirs)

    courses_merged = pd.concat([df_current, df_completed])

    courses_merged = pd.merge(courses_merged, df_codes, how='left', left_on='COURSE_NUMBER', right_on='course_code_l')
    courses_merged = pd.merge(df_students, courses_merged, how='right', on='STUDENT_NUMBER')

    courses_merged = courses_merged[courses_merged['SCHOOL_NAME'].notna()]
    # courses_merged = courses_merged[courses_merged['EXITDATE'].isna()]
    courses_merged = courses_merged.astype({'STUDENT_NUMBER': 'string'})

    # Remove duplicate rows
    courses_merged = courses_merged.drop_duplicates()
    courses_merged = courses_merged[courses_merged['pathway_code'].notna()]
    courses_merged = courses_merged[courses_merged['status'] != 'no credit earned']

    output_file_path = os.path.join(base_dirs['interim'], 'final_course.csv')
    save_dataframe_to_csv(courses_merged, output_file_path)
    return courses_merged


def pathway_course_counts(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Generates a DataFrame with pathway code counts for each student from merged course data,
    then saves the DataFrame to a CSV file.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.

    Returns:
        pd.DataFrame: The output DataFrame with pathway code counts.
    """
    # Generate the merged courses DataFrame
    courses_merged = final_course_df(base_dirs)

    # Select relevant columns
    student_info_columns = ['STUDENT_NUMBER', 'LAST_NAME', 'FIRST_NAME',
                            'COHORTYR', 'GRADE_LEVEL', 'SCHOOL_NAME', 'HOUSE', 'EXITDATE']

    # Group by student and pathway_code, then count
    pathway_counts = courses_merged.groupby(['STUDENT_NUMBER', 'pathway_code']).size().reset_index(name='count')

    # Pivot to create a column for each pathway code
    pathway_counts_pivot = pathway_counts.pivot(index='STUDENT_NUMBER', columns='pathway_code', values='count').fillna(0)

    # Reset index to bring 'STUDENT_NUMBER' back as a column
    pathway_counts_pivot = pathway_counts_pivot.reset_index()

    # Merge with original student information
    student_info = courses_merged[student_info_columns].drop_duplicates()
    pathway_course_counts = pd.merge(student_info, pathway_counts_pivot, on='STUDENT_NUMBER', how='left')

    # # Fill NaNs with 0 for any missing pathway counts
    # pathway_course_counts = pathway_course_counts.fillna(0)

    # Save the DataFrame to a CSV file
    output_file_path = os.path.join(base_dirs['interim'], 'pathway_course_counts.csv')
    save_dataframe_to_csv(pathway_course_counts, output_file_path)

    return pathway_course_counts


def all_source_merge(base_dirs: Dict[str, str]) -> pd.DataFrame:
    """
    Merges students, student_pathway_counts, and naf_students DataFrames on student_number.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.
    """
    students = pull_students_sub(base_dirs)

    pathway_counts = pathway_course_counts(base_dirs)[['STUDENT_NUMBER', 'HC', 'IF', 'JM', 'PS', 'STEM']]
    
    naf_students = pull_naf_students(base_dirs)[['Student ID', 'First Name', 'Last Name', 'Academies', 'Active']]
    naf_students['Student ID'] = naf_students['Student ID'].astype(str).str.strip()
    naf_students = naf_students.rename(columns={'Student ID': 'STUDENT_NUMBER'})
    
    student_agreements = pull_student_agreement_sub(base_dirs)
    student_agreements = student_agreements[student_agreements['district code'] == 'hps']
    student_agreements = student_agreements.rename(columns={'Student ID Number': 'STUDENT_NUMBER'})
    student_agreements = student_agreements.drop(columns=['district code']) 

    wbl_counts = pull_wbl_counts(base_dirs)
    wbl_counts = wbl_counts[wbl_counts['gt_id'].str.startswith('hps')]
    wbl_counts['gt_id'] = wbl_counts['gt_id'].str.replace('hps', '', n=1)  # Remove the initial 'hps' from gt_id
    wbl_counts = wbl_counts.rename(columns={'gt_id': 'STUDENT_NUMBER'})

    internship_counts = pull_internship_counts(base_dirs)
    internship_counts = internship_counts[internship_counts['gt_id'].str.startswith('hps')]
    internship_counts['gt_id'] = internship_counts['gt_id'].str.replace('hps', '', n=1)  # Remove the initial 'hps' from gt_id
    internship_counts = internship_counts.rename(columns={'gt_id': 'STUDENT_NUMBER'})

    # Merging all dataframes on 'STUDENT_NUMBER' and renaming the final merged DataFrame
    pathway_identification = students.merge(pathway_counts, on='STUDENT_NUMBER', how='left')
    pathway_identification = pathway_identification.merge(student_agreements, on='STUDENT_NUMBER', how='left')
    pathway_identification = pathway_identification.merge(wbl_counts, on='STUDENT_NUMBER', how='left')
    pathway_identification = pathway_identification.merge(internship_counts, on='STUDENT_NUMBER', how='left')
    pathway_identification = pathway_identification.merge(naf_students, on='STUDENT_NUMBER', how='outer')

    # Replace all 0 with blank in the final DataFrame
    pathway_identification = pathway_identification.replace(0, '')

    pathway_identification = pathway_identification.drop_duplicates(keep='first')

    output_file_path = os.path.join(base_dirs['processed'], 'pathway_identification.csv')
    save_dataframe_to_csv(pathway_identification, output_file_path)

    duplicates = pathway_identification[pathway_identification.duplicated(keep=False)]
    if not duplicates.empty:
        print("Duplicate rows found:")
        print(duplicates)
    else:
        print("No duplicates found.")

    pathway_identification.to_clipboard(index=False)

    return pathway_identification

def main():
    """
    Main function to execute the data processing steps.
    """
    try:
        base_dirs = {
            'raw': 'data/raw',
            'processed': 'data/processed',
            'interim': 'data/interim'
        }

        pathway_course_counts(base_dirs)
        all_source_merge(base_dirs)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()

