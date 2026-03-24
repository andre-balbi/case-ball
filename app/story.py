from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).parent.parent
OBT = ROOT / "data" / "gold" / "obt.parquet"
BLUE = "#1f77b4"
RED = "#d62728"
GREEN = "#2ca02c"

st.set_page_config(layout="centered", page_title="Ball — Supply Chain Case")

st.markdown("""
<style>
    .block-container { max-width: 820px; padding-top: 2rem; }
    h1 { font-size: 2rem; font-weight: 700; }
    h2 { font-size: 1.3rem; font-weight: 600; margin-top: 2rem; }
    .big-number { font-size: 5rem; font-weight: 800; color: #1f77b4; line-height: 1; }
    .subtitle { font-size: 1.1rem; color: #555; margin-top: 0.5rem; }
    .pipeline-box { background: #f8f9fa; border-left: 4px solid #1f77b4;
                    padding: 1rem 1.5rem; font-family: monospace; font-size: 0.9rem; }
    .alert-card { background: #fff8f0; border-left: 3px solid #ff7f0e;
                  padding: 0.5rem 1rem; margin: 0.3rem 0; font-size: 0.9rem; }
    .rec-card { background: #f0f4ff; border-left: 3px solid #1f77b4;
                padding: 0.6rem 1rem; margin: 0.4rem 0; }
    .footer { font-size: 0.75rem; color: #999; text-align: center; margin-top: 3rem; }
</style>
""", unsafe_allow_html=True)

CHART_CONFIG = {"displayModeBar": False}


@st.cache_data
def load():
    conn = duckdb.connect()
    conn.execute(f"CREATE VIEW obt AS SELECT * FROM read_parquet('{OBT}')")

    otif_global = conn.execute("""
        SELECT
            ROUND(100.0 * SUM(pedidos_on_time) / SUM(total_pedidos), 1) AS otif,
            ROUND(AVG(atraso_medio), 1) AS atraso_medio
        FROM obt WHERE total_pedidos > 0
    """).df()

    otif_mensal = conn.execute("""
        SELECT
            DATE_TRUNC('month', date) AS mes,
            ROUND(100.0 * SUM(pedidos_on_time) / NULLIF(SUM(total_pedidos), 0), 1) AS otif
        FROM obt WHERE total_pedidos > 0
        GROUP BY 1 ORDER BY 1
    """).df()

    otif_trimestral = conn.execute("""
        SELECT
            EXTRACT(quarter FROM date)::INT AS q,
            ROUND(100.0 * SUM(pedidos_on_time) / NULLIF(SUM(total_pedidos), 0), 1) AS otif
        FROM obt WHERE total_pedidos > 0
        GROUP BY 1 ORDER BY 1
    """).df()

    otif_heatmap = conn.execute("""
        SELECT
            EXTRACT(month FROM date)::INT AS mes,
            EXTRACT(year FROM date)::INT AS ano,
            ROUND(100.0 * SUM(pedidos_on_time) / NULLIF(SUM(total_pedidos), 0), 1) AS otif
        FROM obt WHERE total_pedidos > 0
        GROUP BY 1, 2 ORDER BY 2, 1
    """).df()

    otif_etapa = conn.execute("""
        SELECT
            CASE WHEN EXTRACT(day FROM date) <= 10 THEN '1 Inicio'
                 WHEN EXTRACT(day FROM date) <= 20 THEN '2 Meio'
                 ELSE '3 Fim' END AS etapa,
            ROUND(100.0 * SUM(pedidos_on_time) / NULLIF(SUM(total_pedidos), 0), 1) AS otif
        FROM obt WHERE total_pedidos > 0
        GROUP BY 1 ORDER BY 1
    """).df()

    stockout_heatmap = conn.execute("""
        SELECT
            region,
            EXTRACT(quarter FROM date)::INT AS q,
            ROUND(100.0 * SUM(CASE WHEN stockout_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
        FROM obt
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()

    overflow_heatmap = conn.execute("""
        SELECT
            region,
            EXTRACT(quarter FROM date)::INT AS q,
            ROUND(100.0 * SUM(CASE WHEN overflow_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
        FROM obt
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()

    causal = conn.execute("""
        SELECT
            EXTRACT(month FROM date)::INT AS mes,
            ROUND(100.0 * SUM(CASE WHEN stockout_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_stockout,
            ROUND(AVG(CASE WHEN total_pedidos > 0 THEN atraso_medio END), 2) AS atraso_medio
        FROM obt
        GROUP BY 1 ORDER BY 1
    """).df()

    otif_stockout = conn.execute("""
        SELECT
            stockout_flag,
            ROUND(100.0 * SUM(pedidos_on_time) / NULLIF(SUM(total_pedidos), 0), 1) AS otif
        FROM obt WHERE total_pedidos > 0
        GROUP BY 1
    """).df()

    conn.close()
    return otif_global, otif_mensal, otif_trimestral, otif_heatmap, otif_etapa, \
           stockout_heatmap, overflow_heatmap, causal, otif_stockout


(otif_global, otif_mensal, otif_trimestral, otif_heatmap, otif_etapa,
 stockout_heatmap, overflow_heatmap, causal, otif_stockout) = load()

OTIF = otif_global["otif"].iloc[0]
ATRASO = otif_global["atraso_medio"].iloc[0]


# -- Ato 0: Contexto ---------------------------------------------------------

st.markdown("# Supply Chain Analytics — Ball Corporation")
st.markdown(
    "<div class='subtitle'>Jan 2023 – Dez 2024 &nbsp;|&nbsp; "
    "4 regioes &nbsp;|&nbsp; 3 produtos &nbsp;|&nbsp; "
    "8.772 registros diarios</div>",
    unsafe_allow_html=True,
)

st.divider()

# -- Ato 1: Pipeline ---------------------------------------------------------

st.markdown("## Pipeline de Dados")

st.markdown("""
<div class='pipeline-box'>
BRONZE &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
SILVER &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
GOLD<br>
Raw CSVs &nbsp;&nbsp;&nbsp;&rarr;&nbsp;&nbsp;
Limpeza + &nbsp;&nbsp;&rarr;&nbsp;&nbsp;
OBT unica<br>
3 fontes &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
derivacoes &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
tabela<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
deduplicadas &nbsp;&nbsp;&nbsp;&nbsp;
22 colunas
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown("**Alertas de qualidade encontrados:**", unsafe_allow_html=False)
st.markdown("""
<div class='alert-card'>306 pedidos sem data de entrega registrada (4,9% do total)</div>
<div class='alert-card'>123 order_ids duplicados — registros identicos removidos na camada silver</div>
<div class='alert-card'>336 combinacoes (data, regiao, produto) com estoque negativo detectadas</div>
""", unsafe_allow_html=True)

st.divider()

# -- Ato 2: O Problema -------------------------------------------------------

st.markdown("## O Problema")

st.markdown(f"<div class='big-number'>OTIF: {OTIF}%</div>", unsafe_allow_html=True)
st.markdown(
    "<div class='subtitle'>Apenas 1 em cada 6 pedidos e entregue no prazo.</div>",
    unsafe_allow_html=True,
)
st.markdown("<br>**E nao esta melhorando.**", unsafe_allow_html=False)

# sparkline OTIF mensal
worst_idx = otif_mensal["otif"].idxmin()
best_idx = otif_mensal["otif"].idxmax()

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=otif_mensal["mes"], y=otif_mensal["otif"],
    mode="lines", line=dict(color=BLUE, width=2),
    showlegend=False,
))
fig.add_annotation(
    x=otif_mensal.loc[worst_idx, "mes"], y=otif_mensal.loc[worst_idx, "otif"],
    text=f"pior: {otif_mensal.loc[worst_idx, 'otif']}%",
    showarrow=True, arrowhead=2, arrowcolor="#999",
    font=dict(size=11, color=RED), bgcolor="white",
    ax=0, ay=-30,
)
fig.add_annotation(
    x=otif_mensal.loc[best_idx, "mes"], y=otif_mensal.loc[best_idx, "otif"],
    text=f"melhor: {otif_mensal.loc[best_idx, 'otif']}%",
    showarrow=True, arrowhead=2, arrowcolor="#999",
    font=dict(size=11, color=GREEN), bgcolor="white",
    ax=0, ay=30,
)
fig.update_layout(
    height=200, margin=dict(l=0, r=0, t=10, b=0),
    xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
    yaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
    plot_bgcolor="white", paper_bgcolor="white",
)
st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

st.divider()

# -- Ato 3: Quando Acontece --------------------------------------------------

st.markdown("## Quando Acontece")
st.markdown("O problema tem sazonalidade — **Q3 e sistematicamente o pior trimestre.**")

col1, col2 = st.columns(2)

with col1:
    # barras OTIF por trimestre
    q_labels = [f"Q{int(q)}" for q in otif_trimestral["q"]]
    q3_otif = otif_trimestral.loc[otif_trimestral["q"] == 3, "otif"].iloc[0]
    q1_otif = otif_trimestral.loc[otif_trimestral["q"] == 1, "otif"].iloc[0]
    colors_q = [RED if q == 3 else BLUE for q in otif_trimestral["q"]]

    fig = go.Figure(go.Bar(
        x=q_labels, y=otif_trimestral["otif"],
        marker_color=colors_q, showlegend=False,
        text=otif_trimestral["otif"].apply(lambda v: f"{v}%"),
        textposition="outside",
    ))
    fig.add_annotation(
        x="Q3", y=q3_otif,
        text=f"{q1_otif - q3_otif:.1f}pp abaixo do Q1",
        showarrow=True, arrowhead=2, ax=40, ay=-30,
        font=dict(size=11, color=RED),
    )
    fig.update_layout(
        title="OTIF por trimestre (media 2023-2024)",
        height=320, margin=dict(l=0, r=0, t=40, b=0),
        yaxis=dict(range=[0, 25], showgrid=False),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

with col2:
    # heatmap OTIF mes x ano
    pivot = otif_heatmap.pivot(index="ano", columns="mes", values="otif")
    meses = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
             "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=meses,
        y=[str(a) for a in pivot.index],
        colorscale="RdYlGn",
        zmin=13, zmax=22,
        text=pivot.values,
        texttemplate="%{text}%",
        showscale=False,
    ))
    fig.update_layout(
        title="OTIF por mes e ano",
        height=320, margin=dict(l=0, r=0, t=40, b=0),
        xaxis=dict(side="bottom"),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

st.markdown("**No fim do mes e pior do que no inicio.**")

etapa_labels = [e.split(" ", 1)[1] for e in otif_etapa["etapa"]]
fig = go.Figure(go.Bar(
    x=etapa_labels, y=otif_etapa["otif"],
    marker_color=BLUE, showlegend=False,
    text=otif_etapa["otif"].apply(lambda v: f"{v}%"),
    textposition="outside",
    width=0.4,
))
fig.update_layout(
    height=220, margin=dict(l=0, r=0, t=10, b=0),
    yaxis=dict(range=[0, 22], showgrid=False),
    plot_bgcolor="white", paper_bgcolor="white",
)
st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

st.divider()

# -- Ato 4: Onde Acontece ----------------------------------------------------

st.markdown("## Onde Acontece")
st.markdown("**Excesso onde nao e necessario. Falta onde e.**")

col1, col2 = st.columns(2)

def _heatmap_regiao_q(df, title, colorscale, annotation_text, ann_region, ann_q):
    pivot = df.pivot(index="region", columns="q", values="pct")
    q_labels = [f"Q{c}" for c in pivot.columns]
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=q_labels,
        y=pivot.index.tolist(),
        colorscale=colorscale,
        text=pivot.values,
        texttemplate="%{text}%",
        showscale=False,
    ))
    # find coords for annotation
    row_idx = pivot.index.tolist().index(ann_region)
    col_idx = [int(c) for c in pivot.columns].index(ann_q)
    fig.add_annotation(
        x=f"Q{ann_q}", y=ann_region,
        text=annotation_text,
        showarrow=False,
        font=dict(size=9, color="white"),
        bgcolor="rgba(0,0,0,0.5)",
        borderpad=3,
    )
    fig.update_layout(
        title=title,
        height=280, margin=dict(l=0, r=0, t=40, b=0),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    return fig

with col1:
    fig = _heatmap_regiao_q(
        stockout_heatmap, "Stockout% por regiao e trimestre",
        "YlOrRd", "stockout\ncronico", "EU", 4,
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

with col2:
    fig = _heatmap_regiao_q(
        overflow_heatmap, "Overflow% por regiao e trimestre",
        "YlOrRd", "overflow\ncronico", "LATAM", 1,
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

st.markdown(
    "EU apresenta os maiores indices de stockout (Q1 e Q4). "
    "LATAM acumula overflow em todos os trimestres — estoque parado enquanto EU resseca."
)

st.divider()

# -- Ato 5: Por Que Acontece -------------------------------------------------

st.markdown("## Por Que Acontece")
st.markdown("**Stockout causa atraso — e a evidencia e temporal.**")

meses_nome = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
              "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
causal["mes_nome"] = causal["mes"].apply(lambda m: meses_nome[int(m) - 1])

fig = go.Figure()
fig.add_trace(go.Bar(
    x=causal["mes_nome"], y=causal["pct_stockout"],
    name="Stockout%", marker_color=RED, opacity=0.7,
    yaxis="y",
))
fig.add_trace(go.Scatter(
    x=causal["mes_nome"], y=causal["atraso_medio"],
    name="Atraso medio (dias)", mode="lines+markers",
    line=dict(color=BLUE, width=2),
    yaxis="y2",
))

pico_stockout_idx = causal["pct_stockout"].idxmax()
pico_mes = causal.loc[pico_stockout_idx, "mes_nome"]
fig.add_annotation(
    x=pico_mes, y=causal.loc[pico_stockout_idx, "pct_stockout"],
    text="pico de stockout<br>e atraso",
    showarrow=True, arrowhead=2, ax=0, ay=-40,
    font=dict(size=11, color=RED),
)

fig.update_layout(
    height=320, margin=dict(l=0, r=30, t=10, b=0),
    legend=dict(orientation="h", y=1.1),
    yaxis=dict(title="Stockout%", showgrid=False),
    yaxis2=dict(title="Atraso medio (dias)", overlaying="y", side="right", showgrid=False),
    plot_bgcolor="white", paper_bgcolor="white",
    barmode="group",
)
st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

# OTIF atual vs potencial
otif_sem = otif_stockout.loc[~otif_stockout["stockout_flag"], "otif"].iloc[0]
st.markdown(f"**Eliminar stockouts elevaria o OTIF de {OTIF}% para {otif_sem}%.**")

fig = go.Figure(go.Bar(
    x=[OTIF, otif_sem],
    y=["OTIF atual", "OTIF sem stockouts"],
    orientation="h",
    marker_color=[RED, GREEN],
    text=[f"{OTIF}%", f"{otif_sem}%"],
    textposition="outside",
    width=0.4,
))
fig.update_layout(
    height=150, margin=dict(l=0, r=60, t=10, b=0),
    xaxis=dict(range=[0, 22], showgrid=False, showticklabels=False),
    yaxis=dict(showgrid=False),
    plot_bgcolor="white", paper_bgcolor="white",
)
st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

st.divider()

# -- Ato 6: O Que Fazer ------------------------------------------------------

st.markdown("## O Que Fazer")

recomendacoes = [
    ("1", "Rebalancear estoque EU / LATAM",
     "Redistribuir excesso de LATAM para EU reduz stockout sem aumentar custo de compra",
     "Imediato + antes do Q3"),
    ("2", "Aumentar estoque de seguranca no Q2",
     "Q3 e abastecido pelo Q2 — antecipar reposicao antes do pico de demanda",
     "Antes do fim do Q1 de cada ano"),
    ("3", "Revisar SLA de 5 para 7-8 dias",
     "Atraso medio e 3,7 dias — SLA de 5 dias gera falha sistematica",
     "Imediato"),
    ("4", "Auditar producao_capacity no planejamento anual",
     "Dados de capacidade com inconsistencias dificultam projecao de gap",
     "Q4 (ciclo de planejamento)"),
    ("5", "Implementar status_pedido no sistema de pedidos",
     "306 pedidos sem data de entrega impedem calculo real do OTIF",
     "Imediato"),
]

for num, acao, motivo, quando in recomendacoes:
    st.markdown(f"""
<div class='rec-card'>
<strong>{num}. {acao}</strong><br>
<span style='font-size:0.85rem; color:#555'>{motivo}</span><br>
<span style='font-size:0.85rem; color:{BLUE}'><strong>Quando atuar:</strong> {quando}</span>
</div>
""", unsafe_allow_html=True)

st.markdown(
    "<div class='footer'>Fonte: data/gold/obt.parquet &nbsp;|&nbsp; "
    "731 dias &nbsp;|&nbsp; Jan 2023 – Dez 2024</div>",
    unsafe_allow_html=True,
)
