import sqlite3

db_file = 'supply_chain.db'

try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    for table in ['sales_sku', 'purchase_sku']:
        print(f"--- {table} ---")
        cursor.execute(f"PRAGMA table_info('{table}')")
        print([c[1] for c in cursor.fetchall()])
        
    conn.close()
except Exception as e:
    print(e)
