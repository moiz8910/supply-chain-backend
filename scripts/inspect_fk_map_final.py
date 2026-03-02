import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', None)
    
    df = pd.read_excel(excel_file, sheet_name='FK Map')
    print("--- Columns ---")
    print(df.columns.tolist())
    print("\n--- First 5 Rows (Dict) ---")
    print(df.head(5).to_dict(orient='records'))
except Exception as e:
    print(f"Error reading 'FK Map': {e}")
