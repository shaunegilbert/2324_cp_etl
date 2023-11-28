import paramiko
import os
import json
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
        
        # Print the local file path to validate it
        print(f"Downloading file: {file}")
        print(f"Remote path: {remote_directory}/{file}")
        print(f"Local path: {local_file_path}")
        
        sftp.get(remote_directory + "/" + file, local_file_path)
        



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