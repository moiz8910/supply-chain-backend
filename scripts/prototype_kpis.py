import sqlite3
import pandas as pd

db_file = 'supply_chain.db'
log_file = 'kpi_prototype.log'

def log(msg):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')

def run_query(conn, name, sql):
    log(f"\n--- {name} ---")
    try:
        df = pd.read_sql_query(sql, conn)
        log(df.head().to_string())
        log(f"Rows returned: {len(df)}")
        return df
    except Exception as e:
        log(f"Error: {e}")
        return None

try:
    with open(log_file, 'w') as f: f.write("Starting KPI Prototype...\n")
    conn = sqlite3.connect(db_file)
    
    # Check columns
    for table in ['movements', 'material_master', 'orders', 'shipments', 'forecasts']:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table}')")
        cols = [c[1] for c in cursor.fetchall()]
        log(f"{table} columns: {cols}")

    # 1. OTIF Query
    # Linked via movements? 
    # movements: movement_id, shipment_id, order_id, ...
    
    otif_sql = """
    SELECT 
        o.order_id,
        oli.delivery_by_date as promised,
        s.actual_end_date as actual,
        CASE 
            WHEN s.actual_end_date <= oli.delivery_by_date THEN 1 
            ELSE 0 
        END as on_time
    FROM orders o
    JOIN order_line_items oli ON o.order_id = oli.order_id
    JOIN movements m ON m.order_id = o.order_id
    JOIN shipments s ON m.shipment_id = s.shipment_id
    WHERE s.actual_end_date IS NOT NULL
    LIMIT 5
    """
    run_query(conn, "OTIF Logic", otif_sql)
    
    # 2. Forecast Key
    # Product Family in material_master?
    # If not, how to link SKU to Family? 
    # 'forecasts' has 'product_family'.
    
    # Check distinct families in Forecasts
    run_query(conn, "Forecast Families", "SELECT DISTINCT product_family FROM forecasts")

    conn.close()

except Exception as e:
    log(f"Global Error: {e}")
