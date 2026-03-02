import pandas as pd

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
xls = pd.ExcelFile(excel_file)

print("Sheet names:", xls.sheet_names)

# Inspect the first few rows of each sheet to see if one acts as a dictionary
for sheet in xls.sheet_names:
    if 'dictionary' in sheet.lower() or 'schema' in sheet.lower() or 'meta' in sheet.lower():
        print(f"\n--- Possible Dictionary Sheet: {sheet} ---")
        df = pd.read_excel(xls, sheet_name=sheet)
        print(df.head())
        print("-" * 30)

# If no obvious dictionary sheet, print columns of a few data sheets to infer structure
print("\n--- Sample Data Sheet Columns ---")
for sheet in xls.sheet_names[:3]:
    df = pd.read_excel(xls, sheet_name=sheet)
    print(f"Sheet: {sheet}")
    print(df.columns.tolist())
