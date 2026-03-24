from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"


def transform():
    conn = duckdb.connect()

    # --- pedidos: adiciona colunas derivadas, preserva linhas com actual_delivery_date nulo ---
    conn.execute(f"""
        CREATE TABLE orders AS
        SELECT
            *,
            CAST(
                actual_delivery_date - order_date
                AS INTEGER
            )                                                   AS lead_time_days,
            CAST(
                actual_delivery_date - requested_delivery_date
                AS INTEGER
            )                                                   AS delay_days,
            CASE
                WHEN actual_delivery_date IS NULL THEN NULL
                ELSE (actual_delivery_date - requested_delivery_date) <= 0
            END                                                 AS on_time
        FROM read_parquet('{BRONZE}/orders.parquet')
    """)

    # --- producao: deduplica por (date, region, product) e adiciona colunas derivadas ---
    before_prod = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{BRONZE}/production.parquet')"
    ).fetchone()[0]

    conn.execute(f"""
        CREATE TABLE production AS
        WITH deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY date, region, product
                    ORDER BY (SELECT NULL)
                ) AS rn
            FROM read_parquet('{BRONZE}/production.parquet')
        )
        SELECT
            date, plant, region, product, produced_quantity, production_capacity,
            produced_quantity::DOUBLE / production_capacity AS utilization_rate,
            produced_quantity > production_capacity          AS over_capacity
        FROM deduped
        WHERE rn = 1
    """)

    after_prod = conn.execute("SELECT COUNT(*) FROM production").fetchone()[0]
    removed_prod = before_prod - after_prod

    # --- estoque: deduplica por (date, region, product) e adiciona colunas derivadas ---
    before_inv = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{BRONZE}/inventory.parquet')"
    ).fetchone()[0]

    conn.execute(f"""
        CREATE TABLE inventory AS
        WITH deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY date, region, product
                    ORDER BY (SELECT NULL)
                ) AS rn
            FROM read_parquet('{BRONZE}/inventory.parquet')
        )
        SELECT
            date, region, product, stock_level, warehouse_capacity,
            stock_level <= 0                                    AS stockout_flag,
            stock_level > warehouse_capacity                    AS overflow_flag,
            stock_level::DOUBLE / warehouse_capacity            AS warehouse_utilization
        FROM deduped
        WHERE rn = 1
    """)

    after_inv = conn.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]
    removed_inv = before_inv - after_inv

    SILVER.mkdir(parents=True, exist_ok=True)

    conn.execute(f"COPY orders      TO '{SILVER}/orders.parquet'      (FORMAT PARQUET)")
    conn.execute(f"COPY production  TO '{SILVER}/production.parquet'  (FORMAT PARQUET)")
    conn.execute(f"COPY inventory   TO '{SILVER}/inventory.parquet'   (FORMAT PARQUET)")

    conn.close()

    return {
        "production_duplicates_removed": removed_prod,
        "inventory_duplicates_removed": removed_inv,
    }


if __name__ == "__main__":
    transform()
