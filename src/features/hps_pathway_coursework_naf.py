import os
import pandas as pd
import logging
from typing import Dict, List, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_csv_column(file_path: str, columns: List[str]) -> pd.DataFrame:
    """
    Reads specific columns from a CSV file into a DataFrame.

    Args:
        file_path (str): The path to the CSV file.
        columns (List[str]): The list of column names to read.

    Returns:
        pd.DataFrame: The DataFrame containing the specified columns.
    """
    try:
        df = pd.read_csv(file_path, usecols=columns, dtype={"STUDENT_NUMBER": "string"})
        logging.info(f"Successfully read {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"No data in file: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        raise


def subset_dataframe(df: pd.DataFrame, column: str, values: List[Union[str, int]]) -> pd.DataFrame:
    """
    Subsets DataFrame based on values in a specified column.

    Args:
        df (pd.DataFrame): The DataFrame to subset.
        column (str): The column name to filter on.
        values (List[Union[str, int]]): The values to filter for.

    Returns:
        pd.DataFrame: The subset DataFrame.
    """
    try:
        subset_df = df[df[column].isin(values)]
        return subset_df
    except KeyError:
        logging.error(f"Column {column} not found in DataFrame")
        raise
    except Exception as e:
        logging.error(f"Error subsetting DataFrame: {e}")
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
    df = read_csv_column(file_path, columns)
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
    df = read_csv_column(file_path, columns)

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
    df = read_csv_column(file_path, columns)

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

    output_file_path = os.path.join(base_dirs['interim'], 'final_course.csv')
    save_dataframe_to_csv(courses_merged, output_file_path)
    return courses_merged


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

        pull_students_sub(base_dirs)
        pull_course_codes(base_dirs)
        pull_current_courses(base_dirs)
        pull_completed_courses(base_dirs)
        final_course_df(base_dirs)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()

