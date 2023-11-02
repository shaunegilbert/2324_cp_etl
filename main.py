import logging
import traceback
from src.data import sftp_read
from src.utils import sftp_util

def main ():
    try:
        sftp_util.download_files
        
    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        
