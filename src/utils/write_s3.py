import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
import logging

load_dotenv()

s3 = boto3.client('s3')

def upload_to_s3(local_directory, s3_folder_name, bucket):
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            # check if file is a CSV file
            if os.path.splitext(filename)[1] == '.csv':
                # construct full local path
                local_path = os.path.join(root, filename)
                
                # construct s3 path
                relative_path = os.path.relpath(local_path, local_directory).replace("\\","/")
                s3_path = "{}/{}".format(s3_folder_name, relative_path)
                
                try: 
                    s3.upload_file(local_path, bucket, s3_path)
                    logging.info(f"Uploaded {local_path} to {bucket}/{s3_path}")
                    
                except Exception as e:
                    logging.error(f"Unable to upload {local_path} to {bucket}/{s3_path}. Reason: {str(e)}")

def main():
    bucket_name = os.getenv('bucket_name')
    upload_to_s3('data/raw', 'raw', bucket_name)
    upload_to_s3('data/processed', 'processed', bucket_name)

if __name__ == "__main__":
    main()





