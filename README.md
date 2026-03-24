# Supply Chain Analytics - Ball Corporation

Analise de dados operacionais de supply chain (2023-2024) para as regioes LATAM, NA, EU e APAC, produtos A, B e C.

A metrica central e o OTIF (On Time In Full) : utilizado aqui como proxy de pontualidade de entrega (OTD), pois os dados nao registram quantidade entregue, apenas solicitada.

## Stack

| Camada                   | Tecnologia                     |
| ------------------------ | ------------------------------ |
| Transformacao de dados   | Python + DuckDB (SQL embutido) |
| Orquestracao do pipeline | `main.py`                    |
| Dashboard interativo     | Streamlit                      |
| Deploy                   | Modal (conta pessoal)          |

DuckDB foi escolhido por operar localmente sem servidor, ser amplamente utilizado em projetos on-premise, e permitir transformacoes em SQL puro mantendo o codigo familiar e auditavel .

## Execucao

Requer Python 3.10+ e `uv`.

```bash
# instalar dependencias
uv venv --python 3.10 .venv
uv pip install -r requirements.txt

# executar pipeline completo (bronze -> silver -> gold)
.venv/bin/python main.py

# rodar o dashboard localmente
.venv/bin/streamlit run app/dashboard.py --server.port 8000
```

O dashboard consome os artefatos gerados em `data/gold/` e os arquivos silver para calculos de range join. O logo `figs/logo-ball.jpg` esta empacotado no repositorio.

## Estrutura

```
data/
  raw/           # CSVs originais (nao modificar)
  bronze/        # Dados carregados com tipos corretos + quality_report.json
  silver/        # Dados limpos com colunas derivadas (orders, inventory, production)
  gold/          # OBT (One Big Table) e metricas analiticas prontas para consumo
src/
  bronze.py      # Ingestao, validacao de tipos e quality report
  silver.py      # Limpeza, deduplicacao, colunas derivadas (on_time, stockout_flag, etc.)
  gold.py        # Calculo de metricas e construcao da OBT (grain: date x region x product)
app/
  dashboard.py   # Dashboard Streamlit com 4 abas
notebooks/
  bronze.ipynb   # Exploracao da camada bronze
  silver.ipynb   # Exploracao da camada silver
  gold.ipynb     # Exploracao das metricas gold
  insights.ipynb # Analise de insights e recomendacoes priorizadas
docs/
  CONTEXT.md     # Dicionario de dados, metricas, premissas e problemas de qualidade
deploy.py        # Configuracao de deploy na Modal
main.py          # Orquestrador do pipeline
```

## Pipeline de Dados

O pipeline segue a arquitetura medallion (bronze -> silver -> gold):

| Camada | Responsabilidade                                                                                                           |
| ------ | -------------------------------------------------------------------------------------------------------------------------- |
| Bronze | Ingestao dos CSVs com tipagem correta e registro de problemas de qualidade                                                 |
| Silver | Limpeza, deduplicacao e criacao de colunas derivadas (`on_time`, `stockout_flag`, `overflow_flag`, `atraso_medio`) |
| Gold   | Agregacao por `(date, region, product)` na OBT e calculo de metricas de OTIF, estoque e producao                         |

## OBT (One Big Table)

Arquivo principal de analise: `data/gold/obt.parquet`

Grain: `date x region x product`. 8.772 linhas, cobrindo Jan 2023 a Dez 2024.

Colunas principais:

| Coluna                 | Descricao                                                      |
| ---------------------- | -------------------------------------------------------------- |
| `total_pedidos`      | Pedidos realizados naquele dia/regiao/produto                  |
| `pedidos_on_time`    | Pedidos entregues ate a data prometida                         |
| `pedidos_com_atraso` | Pedidos com `actual_delivery_date > requested_delivery_date` |
| `atraso_medio`       | `AVG(actual_delivery_date - requested_delivery_date)` . Media de todos os pedidos do dia/regiao/produto, incluindo os no prazo (valores negativos puxam a media para baixo) |
| `stockout_flag`      | `stock_level <= 0` (inclui backorder)                        |
| `overflow_flag`      | `stock_level > warehouse_capacity`                           |

## Dashboard

O dashboard esta organizado em 4 abas:

| Aba                            | Pergunta respondida                                                         |
| ------------------------------ | --------------------------------------------------------------------------- |
| Qual e o problema?             | Evolucao e distribuicao do OTIF por periodo, regiao e produto               |
| O estoque esta no lugar certo? | Desequilibrio simultaneo (stockout vs overflow) entre regioes               |
| O que causa os atrasos?        | Co-ocorrencia de stockout e atraso; impacto estimado no OTIF via range join |
| O que fazer e quando?          | Recomendacoes priorizadas e simulacao de impacto por SLA                    |

A simulacao de impacto do stockout usa range join (`order_date -> actual_delivery_date`) para identificar pedidos que passaram por pelo menos 1 dia de ruptura no ciclo de entrega . Metodo mais preciso do que o join por `order_date`, que subestima o impacto.

## Notebooks

Os notebooks documentam o racional de cada etapa do pipeline e os insights finais:

- `bronze.ipynb` e `silver.ipynb`: problemas de qualidade encontrados e decisoes de limpeza
- `gold.ipynb`: construcao e validacao da OBT
- `insights.ipynb`: analise completa seguindo a narrativa qualidade de dados -> SLA vs lead time -> desequilibrio geografico -> causalidade stockout/atraso -> recomendacoes

## Dados de Entrada

| Arquivo            | Grain                  | Linhas |
| ------------------ | ---------------------- | ------ |
| `orders.csv`     | por pedido             | 6.282  |
| `production.csv` | regiao x produto x dia | 8.772  |
| `inventory.csv`  | regiao x produto x dia | 8.772  |

Regioes: `LATAM`, `NA`, `EU`, `APAC`. Produtos: `A`, `B`, `C`. Periodo: Jan 2023 a Dez 2024
