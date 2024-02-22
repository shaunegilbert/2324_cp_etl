import pandas as pd
import logging
import os
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def merge_ehps_data ():
    try:
        logging.info('Starting merge for EHPS schools')
        
        #define input and output file paths
        input_path_ehhs = os.path.join('data', 'raw', 'ReadyCT_EHHS_export.csv')
        input_path_syn= os.path.join('data', 'raw', 'ReadyCT_Synergy_export.csv')
        
        output_path = os.path.join('data', 'raw', 'ehps_students.csv')
        
        ehhs = pd.read_csv(input_path_ehhs)
        syn = pd.read_csv(input_path_syn)
        
        ehhs['school'] = 'East Hartford High School'
        syn['school'] = 'Synergy High School'
        
        ehps_students = pd.concat([ehhs, syn])
        
        ehps_students.to_csv(output_path, index=False)
        
        
    except Exception as e:
        logging.error(f'Error during data processing: {e}')
        raise
        
def main():
    try:
        merge_ehps_data()
    except Exception as e:
        print ("Handling error in main: stopping the script")
        raise

if __name__ == "__main__":
    main()