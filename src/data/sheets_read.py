import os
import traceback
from dotenv import load_dotenv
from src.utils.sheets_api_util import get_google_sheets_service, read_sheet, save_sheet_as_csv

def download_sheet_as_csv(spreadsheet_id, range_name, output_filename):
    try:
        service = get_google_sheets_service()

        if not spreadsheet_id or not range_name:
            raise ValueError("SPREADSHEET_ID or RANGE_NAME is missing.")

        sheet_data = read_sheet(spreadsheet_id, range_name, service)

        if not sheet_data:
            raise ValueError(f"No data returned from sheet with ID {spreadsheet_id} and range {range_name}.")

        save_sheet_as_csv(sheet_data, output_filename)
    except Exception as e:
        print(f'Failed to download {output_filename}: {str(e)}')
        traceback.print_exc()
        raise

def main():
    try:
        load_dotenv()  # Load environment variables

        # Student Agreement Sheet
        sa_spreadsheet_id = os.getenv('SA_GS_ID')
        sa_range_name = os.getenv('SA_GS_RANGE')
        download_sheet_as_csv(sa_spreadsheet_id, sa_range_name, 'student_agreement.csv')

        # C3 Attendance Sheet
        c3_spreadsheet_id = os.getenv('C3_GS_ID')
        c3_range_name = os.getenv('C3_GS_RANGE')
        download_sheet_as_csv(c3_spreadsheet_id, c3_range_name, 'c3_attendance.csv')
        
        # C3 Attendance Sheet
        jaws_id = os.getenv('JAWS_ID')
        jaws_range_name = os.getenv('JAWS_RANGE')
        download_sheet_as_csv(jaws_id, jaws_range_name, 'jaws_students.csv')
        
        # C3 Attendance Sheet
        jaws_wbl_id = os.getenv('JAWS_WBL_ID')
        jaws_wbl_range_name = os.getenv('JAWS_WBL_RANGE')
        download_sheet_as_csv(jaws_wbl_id, jaws_wbl_range_name, 'jaws_wbl.csv')
        
        #Internships sheets
        int_id = os.getenv('JAWS_INT_ID')
        int_range = os.getenv('JAWS_INT_RANGE')
        download_sheet_as_csv(int_id, int_range, 'internships.csv')
        
        #check-in report from JAWS
        check_id = os.getenv('CHECK_ID')
        check_range = os.getenv('CHECK_RANGE')
        download_sheet_as_csv(check_id, check_range, 'check_in.csv')
        
        #cert report from JAWS
        cert_id = os.getenv('CERT_ID')
        cert_range = os.getenv('CERT_RANGE')
        download_sheet_as_csv(cert_id, cert_range, 'cert.csv')

        #coursework google sheet
        coursework_id = os.getenv('COURSEWORK_ID')
        coursework_range = os.getenv('COURSEWORK_RANGE')
        download_sheet_as_csv(coursework_id, coursework_range, 'course_codes.csv')

        #naf_students
        naf_id = os.getenv('NAF_ID')
        naf_range =  os.getenv('NAF_RANGE')
        download_sheet_as_csv(naf_id, naf_range, 'naf_students.csv')

    except Exception as e:
        print("Handling error in main: stopping the script")
        traceback.print_exc()
        raise
        

if __name__ == "__main__":
    main()
