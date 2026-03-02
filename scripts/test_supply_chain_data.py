import sqlite3
import pandas as pd
import os
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_file = os.path.join(BASE_DIR, 'data', 'supply_chain.db')
log_file = os.path.join(BASE_DIR, 'scripts', 'test_results.log')

def log(msg):
    print(msg)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')

def run_test(name, func, conn):
    log(f"\n[TEST] {name}...")
    try:
        func(conn)
        log("[PASS] Test completed without errors.")
    except AssertionError as e:
        log(f"[FAIL] {e}")
    except Exception as e:
        log(f"[ERROR] {e}")

def test_fk_integrity(conn):
    """Checks for Foreign Key violations using SQLite's built-in check."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_key_check;")
    violations = cursor.fetchall()
    
    if violations:
        log(f"  -> Found {len(violations)} foreign key violations.")
        # Group by table
        violation_counts = {}
        for v in violations:
            table = v[0]
            violation_counts[table] = violation_counts.get(table, 0) + 1
        
        log("  -> Violations by Table:")
        for t, c in violation_counts.items():
            log(f"     - {t}: {c}")
    else:
        log("  -> No foreign key violations found.")

def test_orphaned_records(conn):
    """Checks for logical orphans that might exist if FKs weren't strictly enforced during load."""
    # Example: Order Lines without Orders
    orphans = pd.read_sql_query("""
        SELECT COUNT(*) as count 
        FROM order_line_items ali 
        LEFT JOIN orders o ON ali."order_id" = o."order_id"
        WHERE o."order_id" IS NULL
    """, conn).iloc[0, 0]
    
    log(f"  -> Order Line Items without valid Order: {orphans}")
    
    # Example: Shipments with invalid Customer
    # assuming 'to_location_customer' links to 'customers'
    # Check if table has this column first
    cols = [r[1] for r in conn.cursor().execute("PRAGMA table_info(shipments)").fetchall()]
    if 'to_location_customer' in cols:
        shipment_orphans = pd.read_sql_query("""
            SELECT COUNT(*) as count
            FROM shipments s
            LEFT JOIN customers c ON s."to_location_customer" = c."customer_id"
            WHERE s."to_location_customer" IS NOT NULL AND c."customer_id" IS NULL
        """, conn).iloc[0, 0]
        log(f"  -> Shipments pointing to invalid Customer: {shipment_orphans}")
    else:
        log("  -> Column 'to_location_customer' not found in shipments.")

def test_data_distribution(conn):
    """Checks distribution of key metrics."""
    # Orders per Region (via Customer?)
    # Schema: Orders(customer_id) -> Customers(customer_region_id) -> Regions(region_id)
    
    query = """
        SELECT r.region_name, COUNT(o.order_id) as order_count
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN regions r ON c.customer_region_id = r.region_id
        GROUP BY r.region_name
    """
    try:
        df = pd.read_sql_query(query, conn)
        log("  -> Orders by Region:")
        log(df.to_string(index=False))
    except Exception as e:
        log(f"  -> Could not run distribution check: {e}")

def main():
    if not os.path.exists(db_file):
        log(f"Database {db_file} not found.")
        return

    # Clear log
    with open(log_file, 'w') as f: f.write("Starting test...\n")

    conn = sqlite3.connect(db_file)
    
    run_test("Foreign Key Integrity", test_fk_integrity, conn)
    run_test("Orphaned Records Logic", test_orphaned_records, conn)
    run_test("Business Logic / Data Distribution", test_data_distribution, conn)
    
    conn.close()

if __name__ == "__main__":
    main()
