import os
from dotenv import load_dotenv
from src.utils.sheets_api_util import( get_google_sheets_service,
                                      clear_and_write_csv_to_sheet)

def processed_c3():
    service = get_google_sheets_service()
    spreadsheet_id = os.getenv('C3_PROCESSED_ID')
    range_name =  os.getenv('C3_PROCESSED_RANGE')
    # csv_file = os.getenv('C3_PROCESSED_PATH')
    csv_file=os.path.join('data', 'processed', 'c3_processed.csv')
    
    clear_and_write_csv_to_sheet(spreadsheet_id, range_name, csv_file, service)

def main():
    # Call your functions here
    processed_c3()

if __name__ == "__main__":
    main()