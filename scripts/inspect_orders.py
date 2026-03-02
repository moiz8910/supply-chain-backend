import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    # Read first 10 rows without header to see layout
    df = pd.read_excel(excel_file, sheet_name='Orders', header=None, nrows=10)
    print("--- First 10 rows of 'Orders' ---")
    print(df.to_string())
except Exception as e:
    print(f"Error: {e}")
