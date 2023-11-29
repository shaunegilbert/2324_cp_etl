import logging
import traceback
from src.data import (sftp_read,
                      sheets_read,
                      sheets_write)
from src.features import c3_wrangle

def main ():
    try:
        sftp_read.main()
        sheets_read.main()
        c3_wrangle.main()
        sheets_write.main()

        
        
    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        
if __name__ == "__main__":
    main()