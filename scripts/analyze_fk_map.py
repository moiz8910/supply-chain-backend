import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    df = pd.read_excel(excel_file, sheet_name='FK Map')
    
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    
    print("Columns found:", df.columns.tolist())
    
    # Iterate and print logic
    # Assuming columns like 'Table', 'Column', 'Target Table', 'Target Column' exists
    # If not, I'll print the first row keys to see what matches
    
    first_row = df.iloc[0]
    print("\nFirst Row Keys:", first_row.keys().tolist())
    print("First Row Values:", first_row.values.tolist())

except Exception as e:
    print(f"Error: {e}")
