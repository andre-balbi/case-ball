from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
SILVER = ROOT / "data" / "silver"
GOLD = ROOT / "data" / "gold"


def calcular_obt(conn):
    # agrega pedidos na granularidade diaria (date, region, product) antes do join
    conn.execute(f"""
        CREATE TABLE pedidos_diarios AS
        SELECT
            order_date                                              AS date,
            region,
            product,
            COUNT(*)                                                AS total_pedidos,
            SUM(quantity)                                           AS quantidade_total,
            SUM(CASE WHEN on_time THEN 1 ELSE 0 END)               AS pedidos_on_time,
            SUM(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END)        AS pedidos_com_atraso,
            SUM(CASE WHEN actual_delivery_date IS NULL THEN 1 ELSE 0 END) AS pedidos_sem_entrega,
            ROUND(AVG(lead_time_days), 2)                           AS lead_time_medio,
            ROUND(AVG(delay_days), 2)                               AS atraso_medio,
            MAX(delay_days)                                         AS atraso_max
        FROM read_parquet('{SILVER}/orders.parquet')
        GROUP BY order_date, region, product
    """)

    conn.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    inv.date,
                    inv.region,
                    inv.product,
                    -- estoque
                    inv.stock_level,
                    inv.warehouse_capacity,
                    inv.stockout_flag,
                    inv.overflow_flag,
                    inv.warehouse_utilization,
                    -- producao
                    prod.plant,
                    prod.produced_quantity,
                    prod.production_capacity,
                    prod.utilization_rate,
                    prod.over_capacity,
                    -- pedidos agregados
                    COALESCE(ped.total_pedidos, 0)          AS total_pedidos,
                    COALESCE(ped.quantidade_total, 0)       AS quantidade_total,
                    COALESCE(ped.pedidos_on_time, 0)        AS pedidos_on_time,
                    COALESCE(ped.pedidos_com_atraso, 0)     AS pedidos_com_atraso,
                    COALESCE(ped.pedidos_sem_entrega, 0)    AS pedidos_sem_entrega,
                    ped.lead_time_medio,
                    ped.atraso_medio,
                    ped.atraso_max,
                    -- balanco: producao + estoque[D-1] - demanda[D]
                    prod.produced_quantity
                        + LAG(inv.stock_level) OVER (
                            PARTITION BY inv.region, inv.product
                            ORDER BY inv.date
                        )
                        - COALESCE(ped.quantidade_total, 0) AS gap_diario
                FROM read_parquet('{SILVER}/inventory.parquet') inv
                LEFT JOIN read_parquet('{SILVER}/production.parquet') prod
                    ON inv.date = prod.date
                    AND inv.region = prod.region
                    AND inv.product = prod.product
                LEFT JOIN pedidos_diarios ped
                    ON inv.date = ped.date
                    AND inv.region = ped.region
                    AND inv.product = ped.product
            )
            SELECT * FROM base
            ORDER BY region, product, date
        ) TO '{GOLD}/obt.parquet' (FORMAT PARQUET)
    """)


def calcular():
    GOLD.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    calcular_obt(conn)
    conn.close()


if __name__ == "__main__":
    calcular()
