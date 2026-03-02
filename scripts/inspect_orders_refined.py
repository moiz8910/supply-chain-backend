import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    
    # Read first 15 rows
    df = pd.read_excel(excel_file, sheet_name='Orders', header=None, nrows=15)
    print("--- First 15 rows of 'Orders' ---")
    for index, row in df.iterrows():
        print(f"Row {index}: {row.values.tolist()}")
        
except Exception as e:
    print(f"Error: {e}")
