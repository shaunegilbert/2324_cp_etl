# sa_read.py
import os
from dotenv import load_dotenv
from src.utils.sheets_api_util import get_google_sheets_service

def read_sheet(spreadsheet_id, range_name, service):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    return values

def download_sheet(spreadsheet_id, range_name):
    load_dotenv()
    service = get_google_sheets_service()
    data = read_sheet(spreadsheet_id, range_name, service)
    if not data:
        print('No data found.')
    else:
        # You can either return this data or save it to a file
        return data

def download_sa():
    SPREADSHEET_ID = os.getenv('SA_GS_ID')
    RANGE_NAME = 'sa_query!A1:AB1'
    sheet_data = download_sheet(SPREADSHEET_ID, RANGE_NAME)
    print(sheet_data)

