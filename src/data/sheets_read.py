# sa_read.py
import os
from dotenv import load_dotenv
from src.utils.sheets_api_util import (get_google_sheets_service,
                                       read_sheet, 
                                       save_sheet_as_csv)


def download_sa():
    service=get_google_sheets_service()
    SPREADSHEET_ID = os.getenv('SA_GS_ID')
    RANGE_NAME = os.getenv('SA_GS_RANGE')
    sheet_data = read_sheet(SPREADSHEET_ID, RANGE_NAME, service)
    
    if sheet_data:
        save_sheet_as_csv(sheet_data, 'student_agreement.csv')
