import logging
import traceback
from src.data import sftp_read
from src.data import sheets_read
import src.features.c3_wrangle

def main ():
    try:
        sftp_read.main()
        sheets_read.download_sa()
        sheets_read.download_c3()
        
    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        
if __name__ == "__main__":
    main()

