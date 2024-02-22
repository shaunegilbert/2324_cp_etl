import os
import traceback
import sys
from dotenv import load_dotenv
from src.utils.sheets_api_util import (get_google_sheets_service,
                                       read_sheet, 
                                       save_sheet_as_csv)


def download_sa():
    try:
        service = get_google_sheets_service()
        SPREADSHEET_ID = os.getenv('SA_GS_ID')
        RANGE_NAME = os.getenv('SA_GS_RANGE')  # This is intentionally incorrect for testing
        if RANGE_NAME is None:
            raise ValueError("RANGE_NAME is None because the environment variable does not exist.")

        sheet_data = read_sheet(SPREADSHEET_ID, RANGE_NAME, service)
        
        if not sheet_data:
            raise ValueError("Sheet data is empty or None.")
        
        save_sheet_as_csv(sheet_data, 'student_agreement.csv')
    except Exception as e:
        print(f'Failed: {str(e)}')
        traceback.print_exc()  # Print the traceback for debugging
        raise  # Ensure the exception is re-raised for propagation

        
def download_c3():
    try:
        service = get_google_sheets_service()
        SPREADSHEET_ID = os.getenv('C3_GS_ID')
        RANGE_NAME = os.getenv('C3_GS_RANGE')

        # Explicit check for environment variable values
        if SPREADSHEET_ID is None or RANGE_NAME is None:
            raise ValueError("SPREADSHEET_ID or RANGE_NAME environment variable is missing.")

        sheet_data = read_sheet(SPREADSHEET_ID, RANGE_NAME, service)

        # Explicit check for the presence of data
        if not sheet_data:
            raise ValueError(f"No data returned from sheet with ID {SPREADSHEET_ID} and range {RANGE_NAME}.")

        save_sheet_as_csv(sheet_data, 'c3_attendance.csv')

    except Exception as e:
        print(f'Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback for debugging
        raise  # Ensure the exception is re-raised for propagation


def main():
    try:
        # Call your functions here
        download_sa()
        download_c3()
    except Exception as e:
        print("Handling error in main: stopping the script")
        raise  # Re-raise the exception

if __name__ == "__main__":
    main()