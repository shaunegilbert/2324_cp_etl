import logging
import traceback
from src.data import (sftp_read,
                      sheets_read,
                      sheets_write,
                      write_to_s3)
from src.features import c3_wrangle
from src.utils.send_email import send_email

def main ():
    try:
        sftp_read.main()
        sheets_read.main()
        c3_wrangle.main()
        sheets_write.main()
        write_to_s3.main()
        
        # Send a success email
        # send_email('shaune.gilbert@readyct.org', 'CP ETL Script Execution Successful', 'The CP ETL script ran successfully.')

        
        
    except Exception as e:
        logging.error(f"Script failed to run. Error: {str(e)}")

        # Send an error email
        # send_email('shaune.gilbert@readyct.org', 'CP ETL Script Execution Failed', f"The CP ETL script failed to run. Error: {str(e)}")
        
        
if __name__ == "__main__":
    main()