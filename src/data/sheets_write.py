import os
import traceback
from dotenv import load_dotenv
from src.utils.sheets_api_util import( get_google_sheets_service,
                                      clear_and_write_csv_to_sheet)

def processed_c3():
    try:
        service = get_google_sheets_service()
        spreadsheet_id = os.getenv('C3_PROCESSED_ID')
        range_name =  os.getenv('C3_PROCESSED_RANGE')
        # csv_file = os.getenv('C3_PROCESSED_PATH')
        csv_file=os.path.join('data', 'processed', 'c3_processed.csv')
        
        
        if not spreadsheet_id or not range_name:
            raise ValueError("Spreadsheet ID or Range name environment variable is missing for processed_c3.")
        
        clear_and_write_csv_to_sheet(spreadsheet_id, range_name, csv_file, service)
        
    except Exception as e:
        print(f'processed_c 3Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback
        raise
    
def hps_sftp_push():
    try:
        service = get_google_sheets_service()
        spreadsheet_id = os.getenv('HPS_STUDENTS_ID')
        range_name = os.getenv('HPS_STUDENTS_RANGE')
        csv_file=os.path.join('data', 'raw', 'students.csv')
        
        if not spreadsheet_id or not range_name:
            raise ValueError("Spreadsheet ID or Range name environment variable is missing for processed_c3.")
        
        clear_and_write_csv_to_sheet(spreadsheet_id, range_name, csv_file, service)
        
    except Exception as e:
        print(f'hps_sftp_push Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback
        raise
    
def ehps_sftp_push():
    try:
        service = get_google_sheets_service()
        spreadsheet_id = os.getenv('EHPS_STUDENTS_ID')
        range_name = os.getenv('EHPS_STUDENTS_RANGE')
        csv_file=os.path.join('data', 'raw', 'ehps_students.csv')
        
        if not spreadsheet_id or not range_name:
            raise ValueError("Spreadsheet ID or Range name environment variable is missing for processed_c3.")
        
        clear_and_write_csv_to_sheet(spreadsheet_id, range_name, csv_file, service)
        
    except Exception as e:
        print(f'ehps_sftp_push Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback
        raise
    
def kpi_student_view_push():
    try:
        service = get_google_sheets_service()
        spreadsheet_id = os.getenv('KPI_STUDENT_VIEW_ID')
        range_name = os.getenv('KPI_STUDENT_VIEW_RANGE')
        csv_file=os.path.join('data', 'processed', 'student_kpi_view.csv')
        
        if not spreadsheet_id or not range_name:
            raise ValueError("Spreadsheet ID or Range name environment variable is missing for kpi_student_view.")
        
        clear_and_write_csv_to_sheet(spreadsheet_id, range_name, csv_file, service)
        
    except Exception as e:
        print(f'kpi_student_view_push Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback
        raise

def main():
    try:
        # Call your functions here
        processed_c3()
        hps_sftp_push()
        ehps_sftp_push()
        kpi_student_view_push()
    except Exception as e:
        print("Handling error in main: stopping the script")
        raise  # Re-raise the exception

if __name__ == "__main__":
    main()