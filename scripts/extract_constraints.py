import pandas as pd
import json

excel_file = 'Supply Chain_Data Dictionary_Company.xlsx'
fk_map = {} # (table, column) -> (target_table, target_column)
pks = {} # table -> pk_column

def normalize(name):
    if not isinstance(name, str): return str(name)
    return name.strip().lower().replace(' ', '_')

try:
    df = pd.read_excel(excel_file, sheet_name='FK Map')
    
    # Clean column names
    df.columns = [c.strip() for c in df.columns]
    
    # Identify relevant columns (adjust based on previous output)
    # Based on "Col 1: 'Table'", "Col 2: 'Column'", "Col 3: 'Reference Table'", "Col 4: 'Reference Column'"
    
    col_table = 'Table'
    col_column = 'Column'
    col_ref_table = 'Reference Table'
    col_ref_col = 'Reference Column'
    
    for _, row in df.iterrows():
        tbl = normalize(row[col_table])
        col = normalize(row[col_column])
        ref_tbl = normalize(row[col_ref_table])
        ref_col_raw = str(row[col_ref_col])
        
        # Clean ref_col (remove " (Primary Key)" etc)
        ref_col = normalize(ref_col_raw.replace('(Primary Key)', '').replace('(Foreign Key)', ''))
        
        # Store FK
        if tbl and col and ref_tbl and ref_col:
            # Foreign Key: tbl.col -> ref_tbl.ref_col
            if tbl not in fk_map: fk_map[tbl] = []
            fk_map[tbl].append({
                'col': col,
                'ref_table': ref_tbl,
                'ref_col': ref_col
            })
            
            # Infer PK on target
            pks[ref_tbl] = ref_col

    print("--- Extracted Constraints ---")
    print(json.dumps({'fks': fk_map, 'inferred_pks': pks}, indent=2))

except Exception as e:
    print(f"Error: {e}")
