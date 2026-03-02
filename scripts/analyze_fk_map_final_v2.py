import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
try:
    df = pd.read_excel(excel_file, sheet_name='FK Map')
    
    print("--- RAW COLUMNS ---")
    for i, col in enumerate(df.columns):
        print(f"Col {i}: '{col}'")
        
    print("\n--- FIRST 3 ROWS ---")
    for index, row in df.head(3).iterrows():
        print(f"Row {index}:")
        for col in df.columns:
            val = row[col]
            print(f"  '{col}': '{val}'")
            
except Exception as e:
    print(f"Error: {e}")
