import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
RAW = ROOT / "data" / "raw"
BRONZE = ROOT / "data" / "bronze"
CSV_OPTS = "header=true, auto_detect=true"


def load_and_validate():
    conn = duckdb.connect()

    conn.execute(f"""
        CREATE TABLE orders AS
        SELECT
            CAST(order_id AS INTEGER)                      AS order_id,
            region,
            product,
            CAST(order_date AS DATE)                       AS order_date,
            CAST(requested_delivery_date AS DATE)          AS requested_delivery_date,
            CAST(actual_delivery_date AS DATE)             AS actual_delivery_date,
            CAST(quantity AS INTEGER)                      AS quantity
        FROM read_csv('{RAW}/orders.csv', {CSV_OPTS})
    """)

    conn.execute(f"""
        CREATE TABLE production AS
        SELECT
            CAST(date AS DATE)                             AS date,
            plant,
            region,
            product,
            CAST(produced_quantity AS INTEGER)             AS produced_quantity,
            CAST(production_capacity AS INTEGER)           AS production_capacity
        FROM read_csv('{RAW}/production.csv', {CSV_OPTS})
    """)

    conn.execute(f"""
        CREATE TABLE inventory AS
        SELECT
            CAST(date AS DATE)                             AS date,
            region,
            product,
            CAST(stock_level AS INTEGER)                   AS stock_level,
            CAST(warehouse_capacity AS INTEGER)            AS warehouse_capacity
        FROM read_csv('{RAW}/inventory.csv', {CSV_OPTS})
    """)

    report = {}

    for table, pk in [
        ("orders",     "order_id"),
        ("production", "date, plant, product"),
        ("inventory",  "date, region, product"),
    ]:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        nulls = {
            col[0]: conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col[0]} IS NULL").fetchone()[0]
            for col in conn.execute(f"DESCRIBE {table}").fetchall()
        }

        dupes = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {pk}, COUNT(*) AS n
                FROM {table}
                GROUP BY {pk}
                HAVING n > 1
            )
        """).fetchone()[0]

        report[table] = {
            "total_rows": total,
            "nulls_per_column": nulls,
            "duplicate_primary_keys": dupes,
        }

    report["inventory"]["negative_stock_records"] = conn.execute(
        "SELECT COUNT(*) FROM inventory WHERE stock_level <= 0"
    ).fetchone()[0]

    report["inventory"]["overflow_records"] = conn.execute(
        "SELECT COUNT(*) FROM inventory WHERE stock_level > warehouse_capacity"
    ).fetchone()[0]

    report["production"]["over_capacity_records"] = conn.execute(
        "SELECT COUNT(*) FROM production WHERE produced_quantity > production_capacity"
    ).fetchone()[0]

    BRONZE.mkdir(parents=True, exist_ok=True)

    for table in ("orders", "production", "inventory"):
        conn.execute(f"COPY {table} TO '{BRONZE}/{table}.parquet' (FORMAT PARQUET)")

    with open(BRONZE / "quality_report.json", "w") as f:
        json.dump(report, f, indent=2)

    conn.close()
    return report


if __name__ == "__main__":
    load_and_validate()
