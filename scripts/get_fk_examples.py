import sqlite3
import json

conn = sqlite3.connect('data/supply_chain.db')
cursor = conn.cursor()
cursor.execute('PRAGMA foreign_key_check;')
violations = cursor.fetchall()
examples = []

for v in violations[:3]:
    table, rowid, ref_table, _ = v
    if table == 'order_line_items':
        cursor.execute(f"SELECT order_id, sku_code, forecast_id, sla_id FROM order_line_items WHERE rowid=?", (rowid,))
        row = cursor.fetchone()
        if row:
            examples.append({
                "table": "order_line_items",
                "order_id": row[0],
                "sku_code": row[1],
                "missing_ref_table": ref_table,
                "missing_id": row[2] if ref_table == 'forecasts' else row[3]
            })

with open('scripts/fk_examples.json', 'w') as f:
    json.dump(examples, f, indent=2)
