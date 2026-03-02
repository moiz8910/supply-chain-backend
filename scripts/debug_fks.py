import sqlite3

conn = sqlite3.connect('data/supply_chain.db')
cursor = conn.cursor()

cursor.execute('PRAGMA foreign_key_check;')
violations = cursor.fetchall()

with open('scripts/debug_fks_output.txt', 'w') as f:
    f.write(f"Total FK Violations Detected: {len(violations)}\n")
    f.write("\nHere are 3 specific examples from the order_line_items table:\n")
    f.write("-" * 50 + "\n")

    count = 0
    for v in violations:
        table, rowid, ref_table, _ = v
        if table == 'order_line_items' and count < 3:
            cursor.execute(f"SELECT * FROM {table} WHERE rowid = ?", (rowid,))
            row = cursor.fetchone()
            
            cursor.execute(f"PRAGMA table_info({table})")
            cols = [info[1] for info in cursor.fetchall()]
            row_dict = dict(zip(cols, row))
            
            f.write(f"\nExample {count+1}:\n")
            f.write(f"  • Order ID : {row_dict.get('order_id')}\n")
            f.write(f"  • SKU Code : {row_dict.get('sku_code')}\n")
            
            if ref_table == 'forecasts':
                f.write(f"  • Violation: Missing Forecast ID '{row_dict.get('forecast_id')}'\n")
                f.write(f"  • Detail   : The ID '{row_dict.get('forecast_id')}' in this order does not exist anywhere in the 'forecasts' table.\n")
            elif ref_table == 'sla_profiles':
                f.write(f"  • Violation: Missing SLA ID '{row_dict.get('sla_id')}'\n")
                f.write(f"  • Detail   : The SLA Profile '{row_dict.get('sla_id')}' listed on this order does not exist in the 'sla_profiles' table.\n")
            
            count += 1
