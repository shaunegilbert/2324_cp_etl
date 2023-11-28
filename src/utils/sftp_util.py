import paramiko
import os
import json
import csv
from dotenv import load_dotenv
from src.utils.get_param import get_parameter

# Load environment variables from .env file
load_dotenv()

def load_credentials(pareter_name):
    try:
        parameter_value = get_parameter(pareter_name)
        
        credentials = json.loads(parameter_value)
        return credentials
    except Exception as e:
        raise Exception(f"Failed to load credentials: {str(e)}")

def establish_sftp_connection(creds):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(creds['hostname'], creds['port'], creds['username'], creds['password'])
    sftp = ssh.open_sftp()
    return sftp

def fetch_files(sftp, directory, file_extension):
    files = sftp.listdir(directory)
    return [file for file in files if file.endswith(file_extension)]

def download_files(sftp, files, remote_directory, local_directory, prefix=""):
    raw_data_path = os.path.expanduser(os.getenv('RAW_DATA_DIR'))
    
    for file in files:
        new_file_name = prefix + file
        local_file_path = os.path.join(raw_data_path, new_file_name)
        
        sftp.get(remote_directory + "/" + file, local_file_path)
        
def convert_files_to_csv(files):
    for file in files:
        if file.endswith('.txt'):
            raw_data_path = os.path.expanduser(os.getenv('RAW_DATA_DIR'))
            txt_file_path = os.path.join(raw_data_path, file)
            csv_file_path = os.path.join(raw_data_path, os.path.splitext(file)[0] + ".csv")
            
            with open(txt_file_path, 'r') as txt_file, open(csv_file_path, 'w', newline='') as csv_file:
                txt_reader = csv.reader(txt_file, delimiter='\t')
                csv_writer = csv.writer(csv_file)
                csv_writer.writerows(txt_reader)

            os.remove(txt_file_path)
        



def close_connection(sftp):
    sftp.close()
    
def main():
    load_credentials()
    establish_sftp_connection()
    fetch_files()
    download_files()
    close_connection()

if __name__ == "__main__":
    main()