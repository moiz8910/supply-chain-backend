import sqlite3
from collections import defaultdict
import os

def generate_report():
    conn = sqlite3.connect('data/supply_chain.db')
    cursor = conn.cursor()

    cursor.execute('PRAGMA foreign_key_check;')
    violations = cursor.fetchall()

    grouped = defaultdict(list)
    for v in violations:
        # v = (table, rowid, parent_table, fkid)
        grouped[(v[0], v[2])].append(v[1])

    report_path = 'data/fk_violations_report.md'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# Foreign Key Violations Report\n\n")
        f.write(f"**Total Violations Detected: {len(violations)}**\n\n")
        f.write("This document lists all records in the database that reference an ID in another table that does not exist.\n\n")
        
        for (table, parent), rowids in grouped.items():
            f.write(f"## Table: `{table}` ➔ Missing from: `{parent}`\n")
            f.write(f"**{len(rowids)} violations**\n\n")
            
            # Identify the specific column causing the violation
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            fk_list = cursor.fetchall()
            fk_cols = []
            for fk in fk_list:
                if fk[2] == parent:
                    fk_cols.append(fk[3])
                    
            if not fk_cols:
                fk_cols = ["Unknown_Column"]
                
            col_str = ", ".join(fk_cols)
            f.write(f"**Foreign Key Column(s):** `{col_str}`\n\n")
            
            f.write("| Row ID | " + " | ".join(fk_cols) + " |\n")
            f.write("|---" * (len(fk_cols) + 1) + "|\n")
            
            for rowid in rowids:
                if fk_cols[0] == "Unknown_Column":
                    f.write(f"| {rowid} | N/A |\n")
                else:
                    try:
                        q = f"SELECT {col_str} FROM {table} WHERE rowid = ?"
                        cursor.execute(q, (rowid,))
                        vals = cursor.fetchone()
                        val_str = " | ".join([str(v) for v in vals]) if vals else "N/A"
                        f.write(f"| {rowid} | {val_str} |\n")
                    except Exception as e:
                        f.write(f"| {rowid} | Error fetching: {e} |\n")
            f.write("\n---\n\n")

    print(f"Report generated successfully at: {os.path.abspath(report_path)}")

if __name__ == '__main__':
    generate_report()
