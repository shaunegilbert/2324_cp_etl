import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

def write_to_s3(path, bucket_name, s3_folder):
    """
    Uploads CSV files from a directory to an S3 bucket
    """
    s3_client = boto3.client('s3')
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):  # Only process CSV files
                try:
                    local_path = os.path.join(root, file)
                    # Adjust the relative path calculation
                    relative_path = os.path.relpath(local_path, start=path)
                    s3_path = os.path.join(s3_folder, relative_path)

                    print(f"Uploading {local_path} to s3://{bucket_name}/{s3_path}")
                    s3_client.upload_file(local_path, bucket_name, s3_path)

                except NoCredentialsError:
                    print(f"Error: No AWS credentials found for uploading {local_path}.")
                    return False
                except ClientError as e:
                    print(f"Error: AWS Client error occurred while uploading {local_path}: {e}")
                    return False
                except Exception as e:
                    print(f"Unexpected error occurred while uploading {local_path}: {e}")
                    return False

    return True

            