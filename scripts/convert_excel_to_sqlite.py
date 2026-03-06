import pandas as pd
import sqlite3
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Configuration
excel_file = os.path.join(BASE_DIR, 'data', 'Supply Chain_Data Dictionary_Company.xlsx')
db_file = os.path.join(BASE_DIR, 'data', 'supply_chain_new.db')
log_file = os.path.join(BASE_DIR, 'scripts', 'migration.log')

def log(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')

import re

def normalize(name):
    if not isinstance(name, str): return str(name)
    name = name.lower().strip()
    name = name.replace(' ', '_').replace('.', '').replace('-', '_')
    
    # Strip any occurrences of _(text_fk) or (pk) or similar metadata suffixes
    name = re.sub(r'_?\([^)]*\)', '', name)
        
    return name

def extract_constraints(xls):
    fk_list = [] # list of (table, col, ref_table, ref_col)
    pks = {} # table -> pk_column_name
    
    try:
        # Check if 'FK Map' exists
        if 'FK Map' not in xls.sheet_names:
            log("Warning: 'FK Map' sheet not found.")
            return pks, fk_list

        df = pd.read_excel(xls, sheet_name='FK Map')
        df.columns = [c.strip() for c in df.columns]
        
        for _, row in df.iterrows():
            if pd.isna(row.get('Table')) or pd.isna(row.get('Column')):
                continue
            
            tbl = normalize(row['Table'])
            col = normalize(row['Column'])
            
            # Check for Reference Table (Foreign Key)
            if 'Reference Table' in df.columns and not pd.isna(row['Reference Table']):
                ref_tbl = normalize(row['Reference Table'])
                # Reference Column often contains "(Primary Key)" text
                ref_col_raw = str(row['Reference Column'])
                ref_col = normalize(ref_col_raw.replace('(Primary Key)', '').replace('(Foreign Key)', ''))
                
                fk_list.append({
                    'table': tbl,
                    'col': col,
                    'ref_table': ref_tbl,
                    'ref_col': ref_col
                })
                
                # Infer PK of target table from this relationship? 
                pks[ref_tbl] = ref_col
            
    except Exception as e:
        log(f"Error parsing FK Map: {e}")
        
    return pks, fk_list

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'REAL'
    else:
        return 'TEXT'

def find_header_row(xls, sheet_name, expected_cols_normalized):
    """
    Scans the first 20 rows to find a row that contains a significant number of expected columns.
    """
    try:
        # Read header-less
        df_scan = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=20)
        
        best_row = 0
        max_matches = 0
        
        for i, row in df_scan.iterrows():
            # Normalize this row's values
            row_values = [normalize(str(x)) for x in row.values if pd.notna(x)]
            
            # Count matches with expected_cols
            matches = sum(1 for x in row_values if x in expected_cols_normalized)
            
            if matches > max_matches:
                max_matches = matches
                best_row = i
        
        # Heuristic: if we found at least 1 match, or if 0 matches but row 0 is default to 0.
        # If we found 0 matches, sticking to 0 is risky if data is garbage there.
        # But if we have NO expected columns (e.g. table not in FK map), we default to 0.
        
        if max_matches > 0:
            return best_row
        return 0
        
    except Exception as e:
        log(f"Error searching header for {sheet_name}: {e}")
        return 0

def convert_excel_to_sqlite(excel_file, db_file):
    # Clear log file
    with open(log_file, 'w') as f: f.write("Starting conversion...\n")

    if not os.path.exists(excel_file):
        log(f"Error: Let's explicitly check input file '{excel_file}'")
        return

    try:
        xls = pd.ExcelFile(excel_file)
        
        # 1. Analyze Schema
        log("Analyzing schema constraints from FK Map...")
        pks, fk_list = extract_constraints(xls)
        
        # Organize FKs by table
        table_fks = {}
        for fk in fk_list:
            table_fks.setdefault(fk['table'], []).append(fk)
        
        # Collect all expected column names per table from FK list (both source and target)
        # to help with header detection.
        expected_cols = {}
        for fk in fk_list:
            expected_cols.setdefault(fk['table'], set()).add(fk['col'])
            # We don't necessarily know other columns of the table, but one is enough to anchor.
            
            # Also for the ref table, we expect the ref_col
            expected_cols.setdefault(fk['ref_table'], set()).add(fk['ref_col'])
        
        # Read all data sheets
        log("Reading data sheets with dynamic header detection...")
        data_frames = {}
        for sheet_name in xls.sheet_names:
            if sheet_name in ['FK Map', 'Data Dictionary', 'About', 'Schema', 'Meta']: 
                continue
            
            # Normalize table name
            tbl = normalize(sheet_name)
            
            # Determine header row
            expected = expected_cols.get(tbl, set())
            header_idx = find_header_row(xls, sheet_name, expected)
            if header_idx > 0:
                log(f"  '{sheet_name}': Detected header at row {header_idx}")
            
            df = pd.read_excel(xls, sheet_name=sheet_name, header=header_idx)
            
            # Normalize columns
            df.columns = [normalize(c) for c in df.columns]
            
            data_frames[tbl] = df
            
            # Try to infer PK if not found (look for 'id' column)
            if tbl not in pks:
                possible_id = [c for c in df.columns if c == 'id' or c == f"{tbl}_id" or c.endswith('_id')]
                if possible_id:
                    # Prefer exact match of tbl_id or id
                    best = next((x for x in possible_id if x == 'id' or x == f"{tbl}_id"), possible_id[0])
                    pks[tbl] = best
                    log(f"Inferred PK for '{tbl}': {pks[tbl]}")

        # Verify PKs exist in actual columns
        log("\nVerifying PK existence...")
        for tbl, pk_col in pks.items():
            if tbl in data_frames:
                if pk_col not in data_frames[tbl].columns:
                    log(f"CRITICAL WARNING: PK '{pk_col}' for table '{tbl}' defined in FK Map, but NOT found in data columns: {data_frames[tbl].columns.tolist()}")
            else:
                 log(f"Warning: Table '{tbl}' defined as PK target but not found in data sheets.")

        # Validate PKs: Ensure all referenced columns are marked as PKs or Unique
        for tbl, fks in table_fks.items():
            for fk in fks:
                ref_tbl = fk['ref_table']
                ref_col = fk['ref_col']
                
                if ref_tbl not in pks:
                    log(f"Warning: '{ref_tbl}' referenced by '{tbl}' but has no PK defined. Setting '{ref_col}' as PK.")
                    pks[ref_tbl] = ref_col
                elif pks[ref_tbl] != ref_col:
                    log(f"Warning: '{ref_tbl}' referenced by '{tbl}' on '{ref_col}', but PK is '{pks[ref_tbl]}'.")

        # 2. Sort Tables by Dependency
        sorted_tables = []
        visited = set()
        
        # Build dependency graph
        deps = {t: set() for t in data_frames}
        for t in data_frames:
            if t in table_fks:
                for fk in table_fks[t]:
                    if fk['ref_table'] in data_frames: # only if ref table exists in our data
                        deps[t].add(fk['ref_table'])

        # Simple topological sort
        while len(sorted_tables) < len(data_frames):
            progress = False
            for t in data_frames:
                if t not in visited:
                    # Check if all deps are visited
                    if all(d in visited for d in deps[t]):
                        visited.add(t)
                        sorted_tables.append(t)
                        progress = True
            
            if not progress:
                log("Warning: Circular dependency detected. Breaking cycle.")
                remaining = [t for t in data_frames if t not in visited]
                if not remaining: break
                t = remaining[0]
                visited.add(t)
                sorted_tables.append(t)
                
        log(f"Table creation order: {sorted_tables}")

        # 3. Create Tables and Insert Data
        if os.path.exists(db_file):
            os.remove(db_file)
            
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        # Disable FK checks for bulk load
        cursor.execute("PRAGMA foreign_keys = OFF;")
        
        for tbl in sorted_tables:
            df = data_frames[tbl]
            
            # Build CREATE TABLE statement
            cols_def = []
            
            # Columns
            seen_cols = set()
            for col_name, dtype in df.dtypes.items():
                original_col_name = col_name
                counter = 1
                while col_name in seen_cols:
                    col_name = f"{original_col_name}_{counter}"
                    counter += 1
                seen_cols.add(col_name)

                sql_type = get_sql_type(dtype)
                
                # Definition parts
                parts = [f'"{col_name}"', sql_type]
                
                # Check PK
                if tbl in pks and pks[tbl] == original_col_name:
                    parts.append("PRIMARY KEY")
                
                cols_def.append(" ".join(parts))
            
            # Foreign Keys
            if tbl in table_fks:
                for fk in table_fks[tbl]:
                    ref_tbl = fk['ref_table']
                    ref_col = fk['ref_col']
                    col = fk['col']
                    
                    # Add FK constraint
                    cols_def.append(f'FOREIGN KEY ("{col}") REFERENCES "{ref_tbl}" ("{ref_col}")')

            if not cols_def:
                log(f"Skipping table '{tbl}' because no valid columns were found.")
                continue

            create_sql = f'CREATE TABLE "{tbl}" (\n  ' + ',\n  '.join(cols_def) + '\n);'
            
            log(f"Creating table '{tbl}'...")
            try:
                cursor.execute(create_sql)
            except sqlite3.OperationalError as e:
                log(f"SQL Error creating '{tbl}': {e}")
                log(f"SQL: {create_sql}")
                continue

            # Data Cleaning: Ensure PK uniqueness and non-null
            if tbl in pks:
                pk_col = pks[tbl]
                if pk_col in df.columns:
                    original_count = len(df)
                    
                    # Drop Null PKs
                    df = df.dropna(subset=[pk_col])
                    null_dropped = original_count - len(df)
                    if null_dropped > 0:
                         log(f"  -> Dropped {null_dropped} rows with NULL PK '{pk_col}' in '{tbl}'.")
                    
                    # Drop Duplicate PKs
                    before_dedupe = len(df)
                    df = df.drop_duplicates(subset=[pk_col], keep='first')
                    dupes_dropped = before_dedupe - len(df)
                    if dupes_dropped > 0:
                        log(f"  -> Dropped {dupes_dropped} duplicate rows for PK '{pk_col}' in '{tbl}'.")

            # Insert Data
            try:
                df.to_sql(tbl, conn, if_exists='append', index=False)
                log(f"  -> Inserted {len(df)} rows into {tbl}.")
            except Exception as e:
                log(f"Error inserting data into '{tbl}': {e}")
                # Try inserting row by row to find the culprit?
                # No, that's too slow. Just log failure.

        # Check for FK violations
        log("\nChecking for foreign key violations...")
        cursor.execute("PRAGMA foreign_key_check;")
        violations = cursor.fetchall()
        if violations:
            log(f"Found {len(violations)} FK violations!")
            # log detailed violations
            for v in violations[:10]:
                 # violation tuple: (table, rowid, referenced_table, constraint_index)
                 log(f"Violation: Table {v[0]} row {v[1]} references {v[2]}")
        else:
            log("No FK violations found.")

        conn.commit()
        conn.close()
        log("\nConversion (with constraints) completed.")

    except Exception as e:
        import traceback
        log(f"Fatal error: {traceback.format_exc()}")

if __name__ == "__main__":
    convert_excel_to_sqlite(excel_file, db_file)
