import os
import csv
import traceback
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from src.utils.sftp_util import (
    load_credentials,
    establish_sftp_connection,
    fetch_files,
    download_files,
    close_connection)

def convert_files_to_csv(files):
    for file in files:
        if file.endswith('.txt'):
            txt_file_path = os.path.join("data", "raw", file)
            csv_file_path = os.path.join("data", "raw", os.path.splitext(file)[0] + ".csv")

            with open(txt_file_path, 'r') as txt_file, open(csv_file_path, 'w', newline='') as csv_file:
                txt_reader = csv.reader(txt_file, delimiter='\t')
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(txt_reader)

            os.remove(txt_file_path)

def ehps_pull():
    try:
        ehps_param = os.getenv('EHPS_SFTP_PARAM')
        ehps_creds = load_credentials(ehps_param)
        ehps_sftp = establish_sftp_connection(ehps_creds)
        ehps_files = fetch_files(ehps_sftp, '/ehps_uploads/', '.txt')
        download_files(ehps_sftp, ehps_files, '/ehps_uploads/', ".", prefix="")

        # Convert EHPS .txt files to CSV
        convert_files_to_csv(ehps_files)

        close_connection(ehps_sftp)
    except Exception as e:
        print(f'Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback

# def hps_pull():
#     try:
#         hps_creds = load_credentials("hps_sftp")
#         hps_sftp = establish_sftp_connection(hps_creds)
#         hps_files = fetch_files(hps_sftp, '/hps_uploads/', '.csv')
#         download_files(hps_sftp, hps_files, '/hps_uploads/', ".", prefix="hps_")

        
#         close_connection(hps_sftp)
#     except Exception as e:
#         print(f'Failed: {str(e)}')

def main():
    ehps_pull()
    # hps_pull()

if __name__ == "__main__":
    main()