# Supply Chain Analytics - Ball Corporation

Analise de dados operacionais de supply chain (2023-2024) para as regioes LATAM, NA, EU e APAC, produtos A, B e C.

## Execucao

Requer Python 3.10+ e `uv`.

```bash
# instalar dependencias
uv venv --python 3.10 .venv
uv pip install -r requirements.txt

# executar pipeline completo
.venv/bin/python main.py
```

O pipeline executa as tres camadas em sequencia e gera todos os arquivos de saida.

### Dashboard Streamlit (local)

Pre-requisito: executar o pipeline (`main.py`) para gerar os artefatos em `data/gold/`.

```bash
# rodar o dashboard localmente
.venv/bin/streamlit run app/dashboard.py --server.port 8000
```

O dashboard consome os artefatos gerados em `data/gold/` e usa o tema claro definido em `.streamlit/config.toml`. O logo `figs/logo-ball.jpg` esta empacotado no repositorio; basta executar localmente, sem configuracao extra.

## Estrutura

```
data/
  raw/           # CSVs originais (nao modificar)
  bronze/        # Dados carregados com tipos corretos + quality_report.json
  silver/        # Dados limpos com colunas derivadas
  gold/          # Metricas analiticas prontas para consumo
src/
  bronze.py      # Ingestao e validacao
  silver.py      # Limpeza, deduplicacao e enriquecimento
  gold.py        # Calculo de metricas (OTIF, estoque, producao, balanco)
notebooks/
  bronze.ipynb   # Documentacao da camada bronze
  silver.ipynb   # Documentacao da camada silver
  gold.ipynb     # Documentacao das metricas gold
  insights.ipynb # Analise de insights e recomendacoes
docs/
  CONTEXT.md     # Dicionario de dados, metricas e premissas
  PRD.md         # Requisitos do projeto
  insights.md    # Insights e recomendacoes (documento final)
main.py          # Orquestrador do pipeline
```

## Saidas da Camada Gold

| Arquivo                        | Conteudo                                          |
| ------------------------------ | ------------------------------------------------- |
| `otif_summary.csv`           | OTIF geral, atraso medio, mediano e P90           |
| `otif_by_region.csv`         | OTIF por regiao                                   |
| `otif_by_product.csv`        | OTIF por produto                                  |
| `otif_monthly.csv`           | OTIF mensal (2023-2024)                           |
| `stockout_summary.csv`       | Frequencia de stockout por regiao/produto         |
| `overflow_summary.csv`       | Frequencia de overflow por regiao/produto         |
| `production_utilization.csv` | Taxa de utilizacao por planta/produto             |
| `supply_demand_gap.csv`      | Gap diario oferta/demanda com flag de gap cronico |

## Resultado Principal

OTIF geral de **17,5%** — problema estrutural sem concentracao em regiao ou produto especifico. Os principais fatores identificados sao desbalanco de estoque entre regioes e SLA de entrega incompativel com o lead time real da operacao. Ver `docs/insights.md` para analise completa e recomendacoes priorizadas.
