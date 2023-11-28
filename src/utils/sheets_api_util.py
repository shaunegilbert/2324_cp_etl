from googleapiclient.discovery import build
from google.oauth2 import service_account
import tempfile
import os
from dotenv import load_dotenv

from src.utils.get_param import get_parameter

def get_google_sheets_service():
    load_dotenv()
    sheets_api_param = os.getenv('SHEETS_API_PARAM')
    service_account_secret = get_parameter(sheets_api_param)

    with tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w') as f:
        f.write(service_account_secret)
        service_account_file = f.name

    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)
    return service

def read_sheet(spreadsheet_id, range_name, service):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        for row in values:
            print(row)