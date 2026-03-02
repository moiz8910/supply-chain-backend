import sqlite3
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_file = os.path.join(BASE_DIR, 'data', 'supply_chain.db')

try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Check for foreign key support
    cursor.execute("PRAGMA foreign_keys;")
    fk_status = cursor.fetchone()[0]
    print(f"Foreign Keys Enabled: {fk_status}") # Should be 0 or 1 depending on connection default, but schema exists regardless.
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print(f"\nFound {len(tables)} tables.")
    
    # Inspect a few tables for constraints
    tables_to_check = ['orders', 'shipments', 'order_line'] # likely to have FKs
    
    for table_name in tables_to_check:
        print(f"\n--- Schema for '{table_name}' ---")
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        schema = cursor.fetchone()
        if schema:
            print(schema[0])
        else:
            print(f"Table '{table_name}' not found.")
            
    # Check row counts again
    print("\n--- Row Counts ---")
    for table in tables:
        t = table[0]
        try:
            c = cursor.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"{t}: {c}")
        except:
            print(f"{t}: Error counting")

    conn.close()

except Exception as e:
    print(f"Verification failed: {e}")
