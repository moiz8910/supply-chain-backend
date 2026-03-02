import pandas as pd
import sys

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'

def normalize(name):
    if not isinstance(name, str): return str(name)
    name = name.lower().strip()
    name = name.replace(' ', '_').replace('.', '').replace('-', '_')
    suffixes = [
        '_(primary_key)', '(primary_key)', 
        '_(foreign_key)', '(foreign_key)',
        '_(fk)', '(fk)',
        '_(pk)', '(pk)',
        '_(natural_key)', '(natural_key)',
        '_(fk_style)',
        '_(region_id_fk)',
        '_(wh_id_fk)', 
    ]
    for s in suffixes:
        name = name.replace(s, '')
    return name

def find_header_row(xls, sheet_name, expected_col):
    if not expected_col: return 0
    try:
        df_scan = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=20)
        best_row = 0
        max_matches = 0
        for i, row in df_scan.iterrows():
            row_values = [normalize(str(x)) for x in row.values if pd.notna(x)]
            matches = sum(1 for x in row_values if expected_col in x) # stricter containment check?
            if matches > max_matches:
                max_matches = matches
                best_row = i
        return best_row
    except: return 0

try:
    xls = pd.ExcelFile(excel_file)
    
    # Check Material Master PK uniqueness
    print("--- Checking 'Material Master' ---")
    # I know from logs that Material Master is 'material_master' and PK is 'sku_code'
    # But I need to find the sheet name.
    # Sheet name is likely 'Material Master'
    
    # Find header first
    header_row = 0
    df_temp = pd.read_excel(xls, sheet_name='Material Master', header=None, nrows=20)
    for i, row in df_temp.iterrows():
        if any('SKU Code' in str(x) for x in row.values):
            header_row = i
            break
            
    df = pd.read_excel(xls, sheet_name='Material Master', header=header_row)
    df.columns = [normalize(c) for c in df.columns]
    
    print(f"Columns: {df.columns.tolist()}")
    
    if 'sku_code' in df.columns:
        dupes = df[df.duplicated('sku_code', keep=False)]
        if not dupes.empty:
            print(f"Duplicate SKU Codes found: {len(dupes)}")
            print(dupes['sku_code'].head())
        else:
            print("SKU Codes are Unique.")
            
        nulls = df['sku_code'].isnull().sum()
        if nulls > 0:
            print(f"Null SKU Codes found: {nulls}")
    else:
        print("Column 'sku_code' not found!")

    # Check Bins vs On Hand Inventory
    # On Hand Inventory references Bins(bin_id)
    print("\n--- Checking 'On Hand Inventory' vs 'Bins' ---")
    
    # Load Bins
    header_row = 0
    df_temp = pd.read_excel(xls, sheet_name='Bins', header=None, nrows=20)
    for i, row in df_temp.iterrows():
        if any('Bin ID' in str(x) for x in row.values):
            header_row = i
            break
    df_bins = pd.read_excel(xls, sheet_name='Bins', header=header_row)
    df_bins.columns = [normalize(c) for c in df_bins.columns]
    bin_ids = set(df_bins['bin_id'].astype(str)) if 'bin_id' in df_bins.columns else set()
    print(f"Loaded {len(bin_ids)} Bin IDs.")
    
    # Load On Hand
    header_row = 0
    df_temp = pd.read_excel(xls, sheet_name='On Hand inventory', header=None, nrows=20)
    for i, row in df_temp.iterrows():
        if any('Batch ID' in str(x) for x in row.values): # trying to find header
            header_row = i
            break
    
    df_oh = pd.read_excel(xls, sheet_name='On Hand inventory', header=header_row)
    df_oh.columns = [normalize(c) for c in df_oh.columns]
    
    if 'bin_id' in df_oh.columns:
        invalid_bins = df_oh[~df_oh['bin_id'].astype(str).isin(bin_ids)]
        print(f"Invalid Bin IDs in Inventory: {len(invalid_bins)}")
        if not invalid_bins.empty:
            print(invalid_bins['bin_id'].head())
            print(f"Example existing bins: {list(bin_ids)[:5]}")
    else:
         print("Column 'bin_id' not found in Inventory!")

except Exception as e:
    import traceback
    traceback.print_exc()
