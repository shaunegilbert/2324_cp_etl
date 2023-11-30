import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_c3_attendance_data():
    try:
        logging.info('Starting the data processing for C3 attendance.')

        # Define input and output file paths
        input_path = '~/dev/2324_cp_etl/data/raw/c3_attendance.csv'
        input_path = os.path.join('data', 'raw', 'c3_attendance.csv')
        output_path = '~/dev/2324_cp_etl/data/processed/c3_processed.csv'
        output_path = os.path.join('data', 'processed', 'c3_processed.csv')


        # Load data into a DataFrame
        df = pd.read_csv(input_path)

        # Create a DataFrame with unique gt_ids
        unique_gt_ids = df[['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory', 'Linked field: gt_id']].drop_duplicates()

        # Pivot the table for attendance
        attendance_pivot = df.pivot_table(
            index=['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory'],
            columns='C3 Lesson Topic',
            values='Linked field: gt_id',
            aggfunc=lambda x: 'Y' if len(x) > 0 else ''
        )

        # Reset index to turn multi-index into columns
        attendance_pivot.reset_index(inplace=True)

        # Merge with unique gt_ids DataFrame
        final_table = pd.merge(unique_gt_ids, attendance_pivot, on=['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory'])

        # Save the processed data
        final_table.to_csv(output_path, index=False)
        logging.info(f'Data processing completed successfully. Output file saved: {output_path}')

    except Exception as e:
        logging.error(f'Error during data processing: {e}')

def main():
    # Call your functions here
    process_c3_attendance_data()
    
if __name__ == "__main__":
    main()

