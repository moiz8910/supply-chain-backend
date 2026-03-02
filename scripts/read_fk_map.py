import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    df = pd.read_excel(excel_file, sheet_name='FK Map')
    print("--- FK Map Content ---")
    print(df.to_string())
except Exception as e:
    print(f"Error reading 'FK Map': {e}")
