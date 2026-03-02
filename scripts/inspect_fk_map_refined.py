import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    df = pd.read_excel(excel_file, sheet_name='FK Map')
    print("--- Columns ---")
    print(df.columns.tolist())
    print("\n--- First 20 Rows ---")
    print(df.head(20).to_string())
except Exception as e:
    print(f"Error reading 'FK Map': {e}")
