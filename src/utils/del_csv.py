import os
import glob
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def delete_all_csvs(directory):
    files_found = glob.glob(f'{directory}/**/*.csv', recursive=True)
    if not files_found:  # Check if the list is empty
        logging.warning(f"No CSV files found in {directory}.")
        return

    for file in files_found:
        try:
            os.remove(file)
            logging.info(f'{file} has been deleted.')
        except OSError as e:
            logging.error(f'Error deleting {file} : {e.strerror}')
            raise RuntimeError(f'Failed to delete one or more files: {e.strerror}') from e

def main():
    try:
        delete_all_csvs('data')
    except RuntimeError as e:
        logging.critical(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()

