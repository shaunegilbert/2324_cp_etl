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


def ehps_pull():
    try:
        ehps_param = os.getenv('EHPS_SFTP_PARAM')
        ehps_remote = os.getenv('EHPS_REMOTE')
        raw_data_path = os.getenv('RAW_DATA_DIR')
        ehps_creds = load_credentials(ehps_param)
        ehps_sftp = establish_sftp_connection(ehps_creds)
        ehps_files = fetch_files(ehps_sftp, ehps_remote, '.txt')
        
        print (ehps_files)
        
        download_files(ehps_sftp, ehps_files, ehps_remote, raw_data_path, prefix="")
        
        close_connection(ehps_sftp)
    except Exception as e:
        print(f'Failed: {str(e)}')
        traceback.print_exc()  # This will print the traceback
        
def main():
    ehps_pull()
    # hps_pull()

if __name__ == "__main__":
    main()