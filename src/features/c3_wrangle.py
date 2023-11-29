import pandas as pd

# Load your data into a DataFrame
df = pd.read_csv('~/dev/2324_cp_etl/data/raw/c3_attendance.csv')  # Replace with your actual data source

# Create a DataFrame with unique gt_ids for each individual
unique_gt_ids = df[['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory', 'Linked field: gt_id']].drop_duplicates()

# Pivot the table for attendance (Y/N)
attendance_pivot = df.pivot_table(
    index=['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory'],
    columns='C3 Lesson Topic',
    values='Linked field: gt_id',
    aggfunc=lambda x: 'Y' if len(x) > 0 else ''
)

# Reset the index to turn multi-index into columns
attendance_pivot.reset_index(inplace=True)

# Merge the attendance pivot table with the unique gt_ids DataFrame
final_table = pd.merge(unique_gt_ids, attendance_pivot, on=['Linked field: Workspace number', 'Linked field: Name', 'Linked field: Subcategory'])

# Optional: Rename the columns if necessary
# final_table.rename(columns={'old_name': 'new_name'}, inplace=True)

# Save or display the pivot table
final_table.to_csv('~/dev/2324_cp_etl/data/processed/c3_processed.csv', index=False)
print(final_table)
