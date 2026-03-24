import sys
from pathlib import Path

# adiciona src/ ao path para importar os modulos do pipeline
sys.path.insert(0, str(Path(__file__).parent / "src"))

import bronze
import silver
import gold


def executar():
    print("Iniciando pipeline bronze -> silver -> gold...")

    print("[1/3] Bronze: carregando e validando dados brutos...")
    relatorio = bronze.load_and_validate()
    print(f"      pedidos: {relatorio['orders']['total_rows']:,} linhas")
    print(f"      producao: {relatorio['production']['total_rows']:,} linhas")
    print(f"      estoque: {relatorio['inventory']['total_rows']:,} linhas")

    print("[2/3] Silver: limpando e enriquecendo dados...")
    resultado = silver.transform()
    print(f"      duplicatas removidas (producao): {resultado['production_duplicates_removed']}")
    print(f"      duplicatas removidas (estoque): {resultado['inventory_duplicates_removed']}")

    print("[3/3] Gold: gerando OBT...")
    gold.calcular()

    print("\nPipeline concluido. Arquivos gerados:")
    print("  data/bronze/ -> orders.parquet, production.parquet, inventory.parquet, quality_report.json")
    print("  data/silver/ -> orders.parquet, production.parquet, inventory.parquet")
    print("  data/gold/   -> obt.parquet")


if __name__ == "__main__":
    executar()
