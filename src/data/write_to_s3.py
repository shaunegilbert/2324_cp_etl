import os
from src.utils.s3_write_util import write_to_s3

def upload_to_s3():
    try:
        bucket_name = os.getenv('S3_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is not set.")

        raw_data_path = os.path.join('data', 'raw')
        processed_data_path = os.path.join('data', 'processed')

        if not os.path.exists(raw_data_path) or not os.path.exists(processed_data_path):
            raise FileNotFoundError("One or more specified paths do not exist.")

        success_raw = write_to_s3(raw_data_path, bucket_name, 'raw/')
        success_processed = write_to_s3(processed_data_path, bucket_name, 'processed/')

        if not success_raw or not success_processed:
            raise Exception("Failed to upload one or more files to S3.")

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except FileNotFoundError as e:
        print(f"File System Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

def main():
    upload_to_s3()

if __name__ == "__main__":
    main()

