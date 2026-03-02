import sqlite3
import pandas as pd

db_file = 'supply_chain.db'

def inspect_columns(conn, table_name):
    print(f"\n--- {table_name} Columns ---")
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        cols = [c[1] for c in cursor.fetchall()]
        print(cols)
        
        # Check for date or status columns specifically
        kpi_cols = [c for c in cols if 'date' in c.lower() or 'status' in c.lower() or 'qty' in c.lower() or 'quantity' in c.lower() or 'cost' in c.lower()]
        if kpi_cols:
            print(f"Potential KPI Columns: {kpi_cols}")
            df = pd.read_sql_query(f"SELECT {', '.join([f'\"{c}\"' for c in kpi_cols])} FROM \"{table_name}\" LIMIT 3", conn)
            print(df.to_string())
            
    except Exception as e:
        print(f"Error: {e}")

try:
    conn = sqlite3.connect(db_file)
    for table in ['orders', 'order_line_items', 'shipments', 'forecasts', 'on_hand_inventory', 'production_runs', 'lines']:
        inspect_columns(conn, table)
    conn.close()
except Exception as e:
    print(f"Failed: {e}")
