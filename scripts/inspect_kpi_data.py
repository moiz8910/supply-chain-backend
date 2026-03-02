import sqlite3
import pandas as pd

db_file = 'supply_chain.db'

def inspect_table(conn, table_name):
    print(f"\n--- {table_name} ---")
    try:
        # Get columns
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        cols = cursor.fetchall()
        col_names = [c[1] for c in cols]
        print(f"Columns: {col_names}")
        
        # Get sample data
        df = pd.read_sql_query(f"SELECT * FROM \"{table_name}\" LIMIT 3", conn)
        print(df.to_string())
    except Exception as e:
        print(f"Error: {e}")

try:
    conn = sqlite3.connect(db_file)
    
    # Tables relevant to KPIs
    inspect_table(conn, 'orders')
    inspect_table(conn, 'order_line_items')
    inspect_table(conn, 'shipments')
    inspect_table(conn, 'forecasts')
    inspect_table(conn, 'on_hand_inventory')
    inspect_table(conn, 'production_runs') # Capacity?
    inspect_table(conn, 'lines') # Capacity?
    
    conn.close()
except Exception as e:
    print(f"Failed: {e}")
