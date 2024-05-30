import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_c3_attendance_data():
    try:
        logging.info('Starting the data processing for C3 attendance.')

        # Define input and output file paths
        # input_path = '~/dev/2324_cp_etl/data/raw/c3_attendance.csv'
        input_path = os.path.join('data', 'raw', 'c3_attendance.csv')
        # output_path = '~/dev/2324_cp_etl/data/processed/c3_processed.csv'
        output_path = os.path.join('data', 'processed', 'c3_processed.csv')
        # #output path for interim c3 data
        # output_path_interim = os.path.join('data', 'interim', 'c3_interim.csv')


        # Load data into a DataFrame
        df = pd.read_csv(input_path)

        # Create a DataFrame with unique gt_ids
        unique_gt_ids = df[['Linked field: Workspace number', 
                            'Linked field: Name', 
                            'Linked field: Subcategory', 
                            'Linked field: gt_id',
                            'Linked field: Pathways 23-24']].fillna('NaN').drop_duplicates()
        
        # Pivot the table for attendance
        attendance_pivot = df.pivot_table(
            index=['Linked field: Workspace number', 
                   'Linked field: Name', 
                   'Linked field: Subcategory'],
            columns='C3 Lesson Topic',
            values='Linked field: gt_id',
            aggfunc=lambda x: 'Y' if len(x) > 0 else ''
        )
        
        # print(attendance_pivot)

        # Reset index to turn multi-index into columns
        attendance_pivot.reset_index(inplace=True)

        # Merge with unique gt_ids DataFrame
        final_table = pd.merge(unique_gt_ids, attendance_pivot, on=['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory'])
        # final_table.to_csv (output_path_interim, index=False)
        
        # Calculate attendance percentage
        # Assuming columns E to N are for attendance, adjust indices as needed
        attendance_columns = final_table.columns[5:15]  # Adjust the indices to match your columns E to N
        final_table['Attendance_Percentage'] = final_table[attendance_columns].apply(lambda row: sum(row == 'Y') / len(attendance_columns), axis=1)

        # Save the processed data
        final_table.to_csv(output_path, index=False)
        logging.info(f'Data processing completed successfully. Output file saved: {output_path}')

    except Exception as e:
        logging.error(f'Error during data processing: {e}')
        raise

def process_ect_c3():
    try:
        logging.info('Starting the data processing for C3 attendance.')

        # Define input and output file paths
        # input_path = '~/dev/2324_cp_etl/data/raw/c3_attendance.csv'
        input_path = os.path.join('data', 'raw', 'ect_c3.csv')
        # output_path = '~/dev/2324_cp_etl/data/processed/c3_processed.csv'
        output_path = os.path.join('data', 'processed', 'ect_c3_processed.csv')
        # #output path for interim c3 data
        # output_path_interim = os.path.join('data', 'interim', 'c3_interim.csv')


        # Load data into a DataFrame
        df = pd.read_csv(input_path)
        # print(df.columns)

        df['Attendance'] = 'Y'

        # Create a DataFrame with unique gt_ids
        unique_gt_ids = df[['Linked field: Workspace number', 
                            'Linked field: Name', 
                            'Linked field: Subcategory',
                            'Linked field: Pipeline',
                            'Linked field: Pipeline start'
                            # 'Attendance'
                            ]].fillna('NaN').drop_duplicates()
        
        # Pivot the table for attendance
        attendance_pivot = df.pivot_table(
            index=['Linked field: Workspace number', 
                   'Linked field: Name', 
                   'Linked field: Subcategory'],
            columns='C3 Lesson Topic',
            values='Attendance',
            aggfunc=lambda x: 'Y' if len(x) > 0 else 'N'
        )
        
        # print(attendance_pivot)

        # Reset index to turn multi-index into columns
        attendance_pivot.reset_index(inplace=True)

        # Merge with unique gt_ids DataFrame
        final_table = pd.merge(unique_gt_ids, attendance_pivot, on=['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory'])
        # print(final_table.columns)
        # print(final_table.columns[5:15])
        # final_table.to_csv (output_path_interim, index=False)
        
        # Calculate attendance percentage
        # Assuming columns E to N are for attendance, adjust indices as needed
        attendance_columns = final_table.columns[5:15]  # Adjust the indices to match your columns E to N
        final_table['Attendance_Percentage'] = final_table[attendance_columns].apply(lambda row: sum(row == 'Y') / len(attendance_columns), axis=1)

        # Save the processed data
        final_table.to_csv(output_path, index=False)
        logging.info(f'Data processing completed successfully. Output file saved: {output_path}')

    except Exception as e:
        logging.error(f'Error during data processing: {e}')
        raise

def main():
    try:
        # Call your functions here
        process_c3_attendance_data()
        process_ect_c3()
    except Exception as e:
        print("Handling error in main: stopping the script")
        raise  # Re-raise the exception
    
if __name__ == "__main__":
    main()

