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
    columns = ['STUDENT_NUMBER', 'LAST_NAME', 'FIRST_NAME', 'COHORTYR', 'GRADE_LEVEL', 'SCHOOL_NAME', 'EXITDATE']
    df = read_csv_column(file_path, columns, dtype={"STUDENT_NUMBER": "string"})
    output_file_path = os.path.join(base_dirs['interim'], 'hps_students_for_coursework.csv')
    save_dataframe_to_csv(df, output_file_path)
    return df


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
    courses_merged = courses_merged[courses_merged['EXITDATE'].isna()]
    courses_merged = courses_merged.astype({'STUDENT_NUMBER': 'string'})

    # Remove duplicate rows
    courses_merged = courses_merged.drop_duplicates()
    courses_merged = courses_merged[courses_merged['pathway_code'].notna()]
    courses_merged = courses_merged[courses_merged['status'] != 'no credit earned']

    output_file_path = os.path.join(base_dirs['interim'], 'final_course.csv')
    save_dataframe_to_csv(courses_merged, output_file_path)
    return courses_merged


def create_student_pathway_count_df(courses_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a DataFrame where each student has an individual row,
    and includes columns for each unique 'pathway_code' with a count
    of how many courses they completed in each pathway.

    Args:
        courses_merged (pd.DataFrame): The input merged DataFrame.

    Returns:
        pd.DataFrame: The output DataFrame with pathway code counts.
    """

    # Select relevant columns
    student_info_columns = ['STUDENT_NUMBER', 'LAST_NAME', 'FIRST_NAME',
                            'COHORTYR', 'GRADE_LEVEL', 'SCHOOL_NAME', 'EXITDATE']

    # Group by student and pathway_code, then count
    pathway_counts = courses_merged.groupby(['STUDENT_NUMBER', 'pathway_code']).size().reset_index(name='count')

    # Pivot to create a column for each pathway code
    pathway_counts_pivot = pathway_counts.pivot(index='STUDENT_NUMBER', columns='pathway_code', values='count').fillna(0)

    # Reset index to bring 'STUDENT_NUMBER' back as a column
    pathway_counts_pivot = pathway_counts_pivot.reset_index()

    # Merge with original student information
    student_info = courses_merged[student_info_columns].drop_duplicates()
    student_pathway_counts = pd.merge(student_info, pathway_counts_pivot, on='STUDENT_NUMBER', how='left')

    # Fill NaNs with 0 for any missing pathway counts
    student_pathway_counts = student_pathway_counts.fillna(0)

    return student_pathway_counts


def process_and_save_pathway_counts(base_dirs: Dict[str, str]):
    """
    Processes the final course DataFrame and creates a DataFrame with pathway code counts for each student.

    Args:
        base_dirs (Dict[str, str]): A dictionary containing base directories.
    """
    courses_merged = final_course_df(base_dirs)
    student_pathway_counts = create_student_pathway_count_df(courses_merged)

    output_file_path = os.path.join(base_dirs['processed'], 'student_pathway_counts.csv')
    save_dataframe_to_csv(student_pathway_counts, output_file_path)


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

        process_and_save_pathway_counts(base_dirs)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()

