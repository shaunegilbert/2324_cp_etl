from googleapiclient.discovery import build
from google.oauth2 import service_account
import tempfile
import os
import csv
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
    # Access the Google Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    # Check if data was found
    if not values:
        print('No data found.')
        return None  # Return None to indicate no data was found
    else:
        # Print each row of the data
        for row in values:
            return values
        # Return the data for further processing if needed
        
    
def save_sheet_as_csv(data, filename):
    raw_data_dir = os.path.expanduser(os.getenv('RAW_DATA_DIR'))
    filepath = os.path.join(raw_data_dir, filename)
    
    # Write data to a CSV file
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(data)

    print(f"Data saved as {filepath}")

# def download_sheet(spreadsheet_id, range_name):
#     load_dotenv()
#     service = get_google_sheets_service()
#     data = read_sheet(spreadsheet_id, range_name, service)
#     if not data:
#         print('No data found.')
#     else:
#         # You can either return this data or save it to a file
#         return data