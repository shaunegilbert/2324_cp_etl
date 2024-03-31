from googleapiclient.discovery import build
from google.oauth2 import service_account
import tempfile
import os
import csv
import logging
from dotenv import load_dotenv
from src.utils.get_param import get_parameter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_google_sheets_service():
    try:
        load_dotenv()
        sheets_api_param = os.getenv('SHEETS_API_PARAM')
        service_account_secret = get_parameter(sheets_api_param)

        # Use the context manager to ensure the file is deleted after use
        with tempfile.NamedTemporaryFile(suffix='.json', mode='w', delete=True) as temp_file:
            temp_file.write(service_account_secret)
            temp_file.flush()  # Ensure data is written to the file

            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = service_account.Credentials.from_service_account_file(temp_file.name, scopes=scopes)
            service = build('sheets', 'v4', credentials=creds)

        # At this point, the temporary file is automatically deleted
        return service
    
    except Exception as e:
        logging.error(f"Failed to get Google Sheets service: {e}")
        return None


def read_sheet(spreadsheet_id, range_name, service):
    try:
        # Access the Google Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])

        # # Print the raw values for debugging
        # print("Raw values returned from API:", values)

        # Check if data was found
        if not values:
            print('No data found.')
            return None  # Return None to indicate no data was found
        else:
            # Print each row of the data
                return values
            # Return the data for further processing if needed
    except Exception as e:
        logging.error(f"Failed to read sheet: {e}")
        return None
        
    
def save_sheet_as_csv(data, filename):
    try:
        raw_data_path = os.path.join('data', 'raw')
        filepath = os.path.join(raw_data_path, filename)
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(data)
        logging.info(f"Data saved as {filepath}")
    except Exception as e:
        logging.error(f"Failed to save sheet as CSV: {e}")

    
# Combined function to clear and write CSV to a sheet
def clear_and_write_csv_to_sheet(spreadsheet_id, range_name, csv_file, service):
    try:
        # Clear existing content
        request = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name)
        request.execute()
        logging.info(f'Sheet {spreadsheet_id} cleared')

        # Write new data from CSV
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)

        body = {'values': data}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption='USER_ENTERED', body=body).execute()
        logging.info(f"{result.get('updatedCells')} cells updated in {spreadsheet_id}.")
    except Exception as e:
        logging.error(f"Failed to clear and write CSV to sheet: {e}")


# def download_sheet(spreadsheet_id, range_name):
#     load_dotenv()
#     service = get_google_sheets_service()
#     data = read_sheet(spreadsheet_id, range_name, service)
#     if not data:
#         print('No data found.')
#     else:
#         # You can either return this data or save it to a file
#         return data