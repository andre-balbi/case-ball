from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT   = Path(__file__).parent.parent
OBT    = ROOT / "data" / "gold" / "obt.parquet"
SILVER = ROOT / "data" / "silver"
LOGO   = ROOT / "figs" / "logo-ball.jpg"
BLUE   = "#1f77b4"
RED    = "#d62728"
GREEN  = "#2ca02c"
ORANGE = "#ff7f0e"

st.set_page_config(
    page_title="Case Ball",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    section[data-testid="stSidebar"] { background-color: #ffffff; }
    :root { color-scheme: light; }
    div[data-testid="stAppViewContainer"] { background-color: #ffffff; color: #262730; }
    div[data-testid="stHeader"] { background-color: #ffffff; }
</style>
""", unsafe_allow_html=True)

CHART_CONFIG = {"displayModeBar": False}


@st.cache_data
def carregar_dados() -> pd.DataFrame:
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM read_parquet('{OBT}')").df()
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data
def carregar_pedidos_com_stockout() -> pd.DataFrame:
    """Para cada pedido com data de entrega, verifica se houve stockout em qualquer dia do ciclo order->delivery."""
    conn = duckdb.connect()
    df = conn.execute(f"""
        WITH ordens AS (
            SELECT order_id, region, product, order_date, actual_delivery_date, on_time
            FROM read_parquet('{SILVER}/orders.parquet')
            WHERE actual_delivery_date IS NOT NULL
        ),
        dias_stockout AS (
            SELECT date, region, product
            FROM read_parquet('{SILVER}/inventory.parquet')
            WHERE stockout_flag = TRUE
        ),
        pedidos_com_stockout AS (
            SELECT DISTINCT o.order_id
            FROM ordens o
            INNER JOIN dias_stockout s
                ON o.region = s.region
                AND o.product = s.product
                AND s.date >= o.order_date
                AND s.date <= o.actual_delivery_date
        )
        SELECT
            o.order_id,
            o.region,
            o.product,
            o.order_date,
            o.on_time,
            CASE WHEN pcs.order_id IS NOT NULL THEN TRUE ELSE FALSE END AS had_stockout
        FROM ordens o
        LEFT JOIN pedidos_com_stockout pcs ON o.order_id = pcs.order_id
    """).df()
    conn.close()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


def aplicar_filtros(df, regioes, produtos, inicio, fim):
    mask = (
        df["region"].isin(regioes)
        & df["product"].isin(produtos)
        & (df["date"] >= inicio)
        & (df["date"] <= fim)
    )
    return df[mask].copy()


def renderizar_sidebar(df):
    if LOGO.exists():
        try:
            st.sidebar.image(str(LOGO), use_container_width=True)
        except TypeError:
            st.sidebar.image(str(LOGO), use_column_width=True)
    st.sidebar.header("Filtros")

    todas_regioes = sorted(df["region"].unique())
    todos_produtos = sorted(df["product"].unique())

    sel_regioes = st.sidebar.multiselect(
        "Regiao",
        options=["Selecionar todas"] + todas_regioes,
        default=["Selecionar todas"],
    )
    regioes = todas_regioes if "Selecionar todas" in sel_regioes else [
        r for r in sel_regioes if r != "Selecionar todas"
    ]

    sel_produtos = st.sidebar.multiselect(
        "Produto",
        options=["Selecionar todos"] + todos_produtos,
        default=["Selecionar todos"],
    )
    produtos = todos_produtos if "Selecionar todos" in sel_produtos else [
        p for p in sel_produtos if p != "Selecionar todos"
    ]

    min_d = df["date"].min().to_pydatetime()
    max_d = df["date"].max().to_pydatetime()
    periodo = st.sidebar.slider(
        "Periodo", min_value=min_d, max_value=max_d,
        value=(min_d, max_d), format="MMM YYYY",
    )
    granularidade = st.sidebar.radio("Granularidade", ["Trimestral", "Mensal"], index=0)

    st.sidebar.markdown("---")
    with st.sidebar.expander("Glossario de termos", expanded=True):
        st.markdown(
            "**OTIF** (On Time In Full)  \nPedidos entregues no prazo e na quantidade certa. Meta: 100%.\n\n"
            "**Stockout**  \nRuptura de estoque: produto sem unidades disponiveis para atender a demanda.\n\n"
            "**Overflow**  \nExcesso de estoque acima da capacidade do armazem. Capital imobilizado.\n\n"
            "**Desequilibrio simultaneo**  \nMesmo produto em stockout em uma regiao e em overflow em outra no mesmo dia.\n\n"
            "**Lead time**  \nTempo total entre o pedido e a entrega ao cliente.\n\n"
            "**SLA** (Service Level Agreement)  \nPrazo maximo contratado para entrega."
        )

    return regioes, produtos, pd.Timestamp(periodo[0]), pd.Timestamp(periodo[1]), granularidade


def kpi_row(cols, metricas):
    for col, (label, valor, delta, delta_color) in zip(cols, metricas):
        col.metric(label, valor, delta=delta, delta_color=delta_color or "normal")


def _add_periodo(df: pd.DataFrame, granularidade: str) -> pd.DataFrame:
    df = df.copy()
    if granularidade == "Trimestral":
        df["periodo"] = "Q" + df["date"].dt.quarter.astype(str)
    else:
        df["periodo"] = df["date"].dt.to_period("M").astype(str)
    return df


# ---------------------------------------------------------------------------
# Tab 1 , Qual e o problema?
# ---------------------------------------------------------------------------

def tab_problema(df: pd.DataFrame, granularidade: str):
    st.subheader("Qual e o problema?")
    st.caption(
        "OTIF (On Time In Full) sintetiza falhas de estoque, producao e logistica em um unico numero. "
        "Os graficos abaixo mostram quando o problema ocorre e quais produtos e regioes sao mais afetados. "
        "Limitacao: os dados nao registram quantidade entregue, apenas solicitada: o indicador mede pontualidade (OTD). O componente 'In Full' nao pode ser aferido."
    )

    d = df[df["total_pedidos"] > 0]
    otif_geral = round(100 * d["pedidos_on_time"].sum() / d["total_pedidos"].sum(), 1)
    otif_q = (
        _add_periodo(d, "Trimestral")
        .groupby("periodo")
        .apply(lambda g: round(100 * g["pedidos_on_time"].sum() / g["total_pedidos"].sum(), 1))
    )
    q3_otif = float(otif_q.get("Q3", 0))
    q1_otif = float(otif_q.get("Q1", 0))
    lead_time = round(5 + d["atraso_medio"].mean(), 1)

    # OTIF por produto
    otif_prod = (
        d.groupby("product")
        .apply(lambda g: round(100 * g["pedidos_on_time"].sum() / g["total_pedidos"].sum(), 1))
    )
    pior_prod  = otif_prod.idxmin()
    pior_valor = float(otif_prod.min())

    cols = st.columns(4)
    kpi_row(cols, [
        ("OTIF Geral", f"{otif_geral}%", None, None),
        ("Lead time medio (dias)", f"{lead_time}", None, "off"),
        ("OTIF Q3 vs media", f"{q3_otif}%", f"{q3_otif - otif_geral:+.1f}pp", "inverse"),
        (f"Pior produto: {pior_prod}", f"{pior_valor}%", None, "off"),
    ])

    st.markdown("---")

    # Linha 1: evolucao temporal + OTIF por produto ordenado
    with st.expander("Por que esses graficos?", expanded=True):
        st.caption(
            "Motivacao: verificar se o OTIF baixo e um problema pontual ou estrutural. "
            "Os dados sugerem que o padrao se repete em todos os periodos, com um trimestre sistematicamente abaixo da media. "
            "Se as premissas de leitura dos dados estiverem corretas, o problema parece estrutural e previsivel, nao um evento isolado."
        )
    col1, col2 = st.columns(2)

    with col1:
        d_p = _add_periodo(d, granularidade)
        otif_p = d_p.groupby("periodo").apply(
            lambda g: round(100 * g["pedidos_on_time"].sum() / g["total_pedidos"].sum(), 1)
        ).reset_index(name="otif")
        label = "trimestre" if granularidade == "Trimestral" else "mes"
        st.markdown(f"**OTIF por {label} , evolucao ao longo do periodo**")

        if granularidade == "Trimestral":
            worst = otif_p.loc[otif_p["otif"].idxmin(), "periodo"]
            colors = [RED if p == worst else BLUE for p in otif_p["periodo"]]
            fig = go.Figure(go.Bar(
                x=otif_p["periodo"], y=otif_p["otif"],
                marker_color=colors,
                text=[f"{v}%" for v in otif_p["otif"]],
                textposition="outside",
            ))
        else:
            fig = go.Figure(go.Scatter(
                x=otif_p["periodo"], y=otif_p["otif"],
                mode="lines+markers", line=dict(color=BLUE, width=2),
                showlegend=False,
            ))
            fig.add_hline(
                y=otif_p["otif"].mean(), line_dash="dash", line_color="#999",
                annotation_text=f"media {otif_p['otif'].mean():.1f}%",
                annotation_position="top right",
            )
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(range=[0, otif_p["otif"].max() * 1.3], showgrid=False),
            xaxis=dict(showgrid=False, tickangle=45 if granularidade == "Mensal" else 0),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    with col2:
        d_p2 = _add_periodo(d, granularidade)
        otif_p2 = d_p2.groupby("periodo").apply(
            lambda g: round(100 * g["pedidos_on_time"].sum() / g["total_pedidos"].sum(), 1)
        ).reset_index(name="otif")
        media_otif = otif_p2["otif"].mean()
        otif_p2["cor"] = otif_p2["otif"].apply(lambda v: RED if v < media_otif else BLUE)
        otif_p2 = otif_p2.sort_values("otif")
        st.markdown(f"**OTIF por {label} , ordenado por desempenho**")

        fig = go.Figure(go.Bar(
            x=otif_p2["otif"], y=otif_p2["periodo"],
            orientation="h",
            marker_color=otif_p2["cor"].tolist(),
            text=[f"{v}%" for v in otif_p2["otif"]],
            textposition="outside",
        ))
        fig.add_vline(
            x=media_otif, line_dash="dash", line_color="#999",
            annotation_text=f"media {media_otif:.1f}%",
            annotation_position="top right",
        )
        fig.update_layout(
            height=300, margin=dict(l=0, r=60, t=10, b=0),
            xaxis=dict(range=[0, max(otif_p2["otif"]) * 1.2], showgrid=False, showticklabels=False),
            yaxis=dict(showgrid=False),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    # Linha 2: heatmap OTIF produto x regiao
    with st.expander("Por que esse grafico?", expanded=True):
        st.caption(
            "Motivacao: verificar se o problema afeta todos os produtos e regioes igualmente ou se ha combinacoes criticas. "
            "Ha heterogeneidade que permite priorizar acoes especificas."
        )
    st.markdown("**OTIF% por produto e regiao , Produto C e EU como pior combinacao**")
    otif_pr = (
        d.groupby(["product", "region"])
        .apply(lambda g: round(100 * g["pedidos_on_time"].sum() / g["total_pedidos"].sum(), 1))
        .unstack("region")
    )
    fig = go.Figure(go.Heatmap(
        z=otif_pr.values,
        x=otif_pr.columns.tolist(),
        y=otif_pr.index.tolist(),
        colorscale="RdYlGn",
        zmin=12, zmax=22,
        text=otif_pr.values,
        texttemplate="%{text}%",
        showscale=True,
    ))
    fig.update_layout(
        height=200, margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)


# ---------------------------------------------------------------------------
# Tab 2 , O estoque esta no lugar errado
# ---------------------------------------------------------------------------

def tab_estoque(df: pd.DataFrame, granularidade: str):
    st.subheader("O estoque esta no lugar errado")
    st.caption(
        "O problema nao e falta de estoque , e distribuicao errada. "
        "Regioes entram em stockout enquanto outras acumulam overflow do mesmo produto no mesmo dia. "
        "Isso indica ausencia de coordenacao entre regioes, nao deficit de producao."
    )

    # KPIs dinamicos
    so_df = df[df["stockout_flag"]][["date", "product", "region"]].rename(columns={"region": "regiao_stockout"})
    ov_df = df[df["overflow_flag"]][["date", "product", "region", "stock_level"]].rename(columns={"region": "regiao_overflow"})
    deseq = so_df.merge(ov_df, on=["date", "product"])
    deseq = deseq[deseq["regiao_stockout"] != deseq["regiao_overflow"]]
    dias_deseq = deseq["date"].nunique()
    unid_paradas = int(deseq["stock_level"].mean()) if not deseq.empty else 0

    stockout_dias_regiao = (
        df[df["stockout_flag"]].groupby("region")["date"].nunique()
    )
    pior_so_regiao = stockout_dias_regiao.idxmax() if not stockout_dias_regiao.empty else "-"
    dias_so_pior = int(stockout_dias_regiao.max()) if not stockout_dias_regiao.empty else 0

    cols = st.columns(3)
    kpi_row(cols, [
        (f"Dias {pior_so_regiao} em stockout", f"{dias_so_pior}", None, "off"),
        ("Dias desequilibrio simultaneo", f"{dias_deseq}", None, "off"),
        ("Estoque medio parado em overflow (un.)", f"{unid_paradas:,}", None, "off"),
    ])

    st.markdown("---")

    # Stockout e overflow lado a lado por regiao
    with st.expander("Por que esses graficos?", expanded=True):
        st.caption(
            "Motivacao: mapear onde o estoque falha , ruptura ou excesso , por regiao e periodo. "
            "O problema nao e uniforme; algumas regioes sofrem stockout cronico enquanto outras acumulam overflow nos mesmos periodos."
        )
    col1, col2 = st.columns(2)

    def _heatmap_flag(col_flag: str, title: str):
        d_p = _add_periodo(df, granularidade)
        pivot = (
            d_p.groupby(["region", "periodo"])[col_flag]
            .mean().mul(100).round(1).unstack("periodo")
        )
        fig = go.Figure(go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale="YlOrRd",
            text=pivot.values, texttemplate="%{text}%",
            showscale=False,
        ))
        fig.update_layout(
            title=title, height=260,
            margin=dict(l=0, r=0, t=40, b=0),
            xaxis=dict(tickangle=45 if granularidade == "Mensal" else 0),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        return fig

    with col1:
        st.plotly_chart(_heatmap_flag("stockout_flag", "Stockout% por regiao"), use_container_width=True, config=CHART_CONFIG)
    with col2:
        st.plotly_chart(_heatmap_flag("overflow_flag", "Overflow% por regiao"), use_container_width=True, config=CHART_CONFIG)

    # Desequilibrio simultaneo por produto
    with st.expander("Como ler esse grafico?", expanded=True):
        st.caption(
            "Cada barra representa uma regiao que tinha EXCESSO de estoque (overflow) de um produto "
            "enquanto outra regiao qualquer estava em RUPTURA (stockout) desse mesmo produto no mesmo dia. "
            "O eixo X mostra quantos dias unicos isso aconteceu. "
            "A legenda 'Regiao com overflow' identifica quem tinha estoque sobrando e poderia ter redistribuido. "
            "A regiao que estava em falta nao aparece neste grafico , mas qualquer uma delas pode ser a origem da ruptura. "
            "Onde o desequilibrio e mais intenso, o problema nao e falta de producao , e falta de redistribuicao entre regioes."
        )
    st.markdown("**Dias com desequilibrio simultaneo , stockout em uma regiao e overflow em outra, mesmo produto, mesmo dia**")
    if not deseq.empty:
        deseq_prod = (
            deseq.groupby(["product", "regiao_overflow"])
            .agg(dias=("date", "nunique"), estoque_medio=("stock_level", "mean"))
            .reset_index()
        )
        deseq_prod["estoque_medio"] = deseq_prod["estoque_medio"].round(0).astype(int)
        deseq_prod = deseq_prod.sort_values(["product", "dias"], ascending=[True, False])

        fig = px.bar(
            deseq_prod, x="dias", y="product", color="regiao_overflow",
            barmode="group", orientation="h",
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={"dias": "Dias de desequilibrio", "product": "Produto", "regiao_overflow": "Regiao com overflow"},
        )
        fig.update_layout(
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False),
            plot_bgcolor="white", paper_bgcolor="white",
            legend_title="Regiao com overflow",
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    # Scatter: stockout% vs overflow% por produto x regiao
    with st.expander("Por que esse grafico?", expanded=True):
        st.caption(
            "Motivacao: o desequilibrio de estoque so e um problema real se stockout e overflow ocorrem AO MESMO TEMPO no mesmo produto , "
            "caso contrario podem ser sazonalidades distintas sem relacao entre si. "
            "Como ler: o label 'A → B' significa que A tem excesso (overflow) e deveria redistribuir para B, que esta em stockout no mesmo dia. "
            "Cada ponto e por definicao um evento simultaneo confirmado. "
            "Os pares com maior concentracao de dias e unidades paradas indicam onde uma redistribuicao possivelmente reduziria a ruptura sem necessidade de aumentar producao, sujeito a validacao operacional."
        )
    st.markdown("**Dias e volume de desequilibrio simultaneo por par de regioes , mesmo produto, mesmo dia**")

    if not deseq.empty:
        deseq_scatter = (
            deseq.groupby(["product", "regiao_stockout", "regiao_overflow"])
            .agg(
                dias=("date", "nunique"),
                unidades_paradas=("stock_level", "mean"),
            )
            .reset_index()
        )
        deseq_scatter["par"] = deseq_scatter["regiao_overflow"] + " → " + deseq_scatter["regiao_stockout"]
        deseq_scatter["unidades_paradas"] = deseq_scatter["unidades_paradas"].round(0).astype(int)

        fig = px.scatter(
            deseq_scatter,
            x="unidades_paradas", y="dias",
            color="product", text="par",
            color_discrete_map={"A": BLUE, "B": GREEN, "C": RED},
            labels={
                "unidades_paradas": "Estoque medio parado na regiao com overflow (unidades)",
                "dias": "Dias com desequilibrio simultaneo",
                "product": "Produto",
            },
        )
        media_dias = deseq_scatter["dias"].mean()
        media_unid = deseq_scatter["unidades_paradas"].mean()

        fig.update_traces(marker=dict(size=12), textposition="top center")
        fig.add_hline(y=media_dias, line_dash="dash", line_color="#bbb",
                      annotation_text="media dias", annotation_position="right")
        fig.add_vline(x=media_unid, line_dash="dash", line_color="#bbb",
                      annotation_text="media unidades", annotation_position="top")
        fig.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False),
            plot_bgcolor="white", paper_bgcolor="white",
            legend_title="Produto",
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)
        st.caption("Leitura: 'A → B' = A tem excesso e deveria redistribuir para B (em stockout no mesmo dia). X = estoque medio parado em A; Y = quantos dias o desequilibrio ocorreu.")

    # Correlacao entre regioes (ausencia de coordenacao)
    with st.expander("Por que esse grafico?", expanded=True):
        st.caption(
            "Motivacao: verificar se o stock_level diario de diferentes regioes tem alguma relacao estatistica para o mesmo produto. "
            "Limitacao importante: o OBT nao tem dados de reposicao , entao nao e possivel deduzir quando ou se uma regiao recebeu abastecimento. "
            "A correlacao mede apenas se os niveis sobem e descem juntos no mesmo dia , o que pode ser baixo ate em cadeias coordenadas se as demandas regionais forem diferentes. "
            "Este grafico e um sinal fraco; o indicador mais direto de possivel descoordinacao esta no grafico anterior."
        )
    st.markdown("**Correlacao de Pearson do stock_level diario entre regioes , por produto**")
    st.caption("Cada celula = correlacao entre o stock_level diario de duas regioes para o mesmo produto. Sinal fraco por si so , use em conjunto com o grafico de desequilibrio simultaneo.")

    produtos_sel = sorted(df["product"].unique())
    cols_corr = st.columns(len(produtos_sel))
    for col, prod in zip(cols_corr, produtos_sel):
        pivot = (
            df[df["product"] == prod]
            .groupby(["date", "region"])["stock_level"]
            .mean().unstack("region")
        )
        corr = pivot.corr().round(2)
        import numpy as np
        mask_vals = corr.values.copy().astype(float)
        # only lower triangle
        for i in range(len(mask_vals)):
            for j in range(i, len(mask_vals)):
                mask_vals[i][j] = None

        fig = go.Figure(go.Heatmap(
            z=mask_vals,
            x=corr.columns.tolist(),
            y=corr.index.tolist(),
            colorscale="RdYlGn",
            zmin=-1, zmax=1,
            text=corr.values,
            texttemplate="%{text}",
            showscale=False,
        ))
        fig.update_layout(
            title=f"Produto {prod}",
            height=240, margin=dict(l=0, r=0, t=35, b=0),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        col.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)


# ---------------------------------------------------------------------------
# Tab 3 , O que causa os atrasos graves?
# ---------------------------------------------------------------------------

def tab_causas(df: pd.DataFrame, granularidade: str):
    st.subheader("O que causa os atrasos graves?")
    st.caption(
        "Se stockout e atraso co-ocorrem nos mesmos periodos, ha indicio de causalidade temporal, mas os dados nao provam relacao direta de causa e efeito. "
        "Uma parcela significativa dos dias do calendario registra algum stockout em alguma regiao. "
        "Quando rastreado pelo ciclo completo de entrega (order_date ate actual_delivery_date), uma parcela dos pedidos passa por pelo menos um dia de stockout, com OTIF inferior ao grupo sem stockout. "
        "Se essa associacao for causal, o ganho potencial ao eliminar rupturas e calculado dinamicamente no grafico com base nos filtros ativos."
    )

    d = df[df["total_pedidos"] > 0]
    pct_atraso = round(100 * d["pedidos_com_atraso"].sum() / d["total_pedidos"].sum(), 1)
    dias_total   = df["date"].nunique()
    dias_stockout = df[df["stockout_flag"]]["date"].nunique()
    pct_stockout = round(100 * dias_stockout / dias_total, 1) if dias_total > 0 else 0
    otif_by_so = d.groupby("stockout_flag").apply(
        lambda g: round(100 * g["pedidos_on_time"].sum() / g["total_pedidos"].sum(), 1)
    )
    otif_s  = float(otif_by_so.get(True, 0))
    otif_ns = float(otif_by_so.get(False, 0))

    cols = st.columns(3)
    kpi_row(cols, [
        ("Pedidos com atraso", f"{pct_atraso}%", None, "off"),
        ("Dias com stockout", f"{pct_stockout}%", None, "off"),
        ("OTIF: sem vs com stockout", f"{otif_ns}% vs {otif_s}%", None, "off"),
    ])
    st.caption(
        "Definicao adotada: stockout_flag = stock_level <= 0 (ja inclui backorder). "
        "A maioria dos registros de stockout corresponde a estoque negativo, nao a ruptura pontual com zero unidades."
    )

    st.markdown("---")

    # Dual-axis: stockout + atraso por periodo
    label = "trimestre" if granularidade == "Trimestral" else "mes"
    with st.expander("O que esse grafico me fornece?", expanded=True):
        st.caption(
            "Este grafico sobrepoe dois indicadores no mesmo eixo de tempo: a porcentagem de dias com stockout (barras, eixo esquerdo) "
            "e o atraso medio de entrega em dias (linha, eixo direito). "
            "Se os dois sobem e descem juntos nos mesmos periodos, ha evidencia temporal de que a ruptura de estoque precede ou coincide com os atrasos. "
            "Nao prova causalidade direta, mas e o tipo de padrao que justifica investigar stockout como possivel causa raiz antes de outras hipoteses."
        )
    st.markdown(f"**Stockout e atraso co-ocorrem por {label} , possivel indicativo de causalidade**")

    causal = (
        _add_periodo(df, granularidade)
        .groupby("periodo")
        .agg(
            pct_stockout=("stockout_flag", lambda x: round(100 * x.mean(), 1)),
            atraso_medio=("atraso_medio", lambda x: round(x.mean(), 2)),
        )
        .reset_index()
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=causal["periodo"], y=causal["pct_stockout"],
        name="Stockout%", marker_color=RED, opacity=0.7, yaxis="y",
    ))
    fig.add_trace(go.Scatter(
        x=causal["periodo"], y=causal["atraso_medio"],
        name="Atraso medio (dias)", mode="lines+markers",
        line=dict(color=BLUE, width=2), yaxis="y2",
    ))
    pico_idx = causal["pct_stockout"].idxmax()
    fig.add_annotation(
        x=causal.loc[pico_idx, "periodo"],
        y=causal.loc[pico_idx, "pct_stockout"],
        text="pico de stockout",
        showarrow=True, arrowhead=2, ax=0, ay=-35,
        font=dict(size=11, color=RED),
    )
    fig.update_layout(
        height=300, margin=dict(l=0, r=30, t=10, b=0),
        legend=dict(orientation="h", y=1.1),
        yaxis=dict(title="Stockout%", showgrid=False),
        yaxis2=dict(title="Atraso medio (dias)", overlaying="y", side="right", showgrid=False),
        xaxis=dict(tickangle=45 if granularidade == "Mensal" else 0),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    with st.expander("O que esses graficos me fornecem?", expanded=True):
        st.caption(
            "Grafico da esquerda: mostra a taxa de stockout (% de dias com ruptura) separada por produto e periodo. "
            "Grafico da direita: simula o OTIF total se todos os pedidos que passaram por stockout no seu ciclo (order_date ate actual_delivery_date) tivessem a mesma taxa dos pedidos sem stockout. "
            "Metodo: range join pedido x inventario. Assumindo que a associacao e causal, o ganho estimado e calculado dinamicamente no grafico com base nos filtros ativos. "
            "O principal driver do OTIF baixo parece ser o desalinhamento entre o SLA contratado e o lead time real, afetando a maioria dos pedidos independentemente de stockout."
        )
    col1, col2 = st.columns(2)

    # Stockout por produto x periodo
    with col1:
        st.markdown(f"**Stockout% por produto e {label} , Produto C lidera as rupturas**")
        stockout_pp = (
            _add_periodo(df, granularidade)
            .groupby(["product", "periodo"])["stockout_flag"]
            .mean().mul(100).round(1).reset_index()
        )
        fig = px.bar(
            stockout_pp, x="periodo", y="stockout_flag", color="product",
            barmode="group",
            color_discrete_map={"A": BLUE, "B": GREEN, "C": RED},
            labels={"stockout_flag": "Stockout%", "periodo": "", "product": "Produto"},
        )
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(showgrid=False),
            xaxis=dict(tickangle=45 if granularidade == "Mensal" else 0),
            plot_bgcolor="white", paper_bgcolor="white",
            legend_title="Produto",
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    # OTIF atual vs potencial (simulacao via range join pedido x inventario)
    with col2:
        st.markdown("**OTIF atual vs potencial sem stockout**")
        otif_atual = round(100 * d["pedidos_on_time"].sum() / d["total_pedidos"].sum(), 1)

        # Simulacao: pedidos que passaram por stockout no ciclo order->delivery recebem a taxa dos pedidos sem stockout
        # Base consistente: on_time_atual vem do OBT (6.282 pedidos, incluindo 306 sem entrega como falha)
        # Ganho = quantos pedidos a mais chegariam no prazo se o grupo stockout tivesse a taxa do grupo sem stockout
        pedidos_so = carregar_pedidos_com_stockout()
        regioes_ativas = df["region"].unique()
        produtos_ativos = df["product"].unique()
        pedidos_f = pedidos_so[
            pedidos_so["region"].isin(regioes_ativas)
            & pedidos_so["product"].isin(produtos_ativos)
            & (pedidos_so["order_date"] >= df["date"].min())
            & (pedidos_so["order_date"] <= df["date"].max())
        ]
        p_so  = pedidos_f[pedidos_f["had_stockout"]]
        p_nso = pedidos_f[~pedidos_f["had_stockout"]]
        otif_ns_rate = p_nso["on_time"].mean() if len(p_nso) > 0 else 0
        otif_so_rate  = p_so["on_time"].mean()  if len(p_so)  > 0 else 0
        on_time_atual  = d["pedidos_on_time"].sum()
        total_pedidos  = d["total_pedidos"].sum()
        extra_on_time  = len(p_so) * (otif_ns_rate - otif_so_rate)
        otif_potencial = round(100 * (on_time_atual + extra_on_time) / total_pedidos, 1) if total_pedidos > 0 else otif_atual

        fig = go.Figure(go.Bar(
            x=[otif_atual, otif_potencial],
            y=["OTIF atual", "OTIF sem stockouts"],
            orientation="h",
            marker_color=[RED, GREEN],
            text=[f"{otif_atual}%", f"{otif_potencial}%"],
            textposition="outside",
            width=0.4,
        ))
        fig.add_annotation(
            x=(otif_atual + otif_potencial) / 2, y=0.5,
            text=f"+{round(otif_potencial - otif_atual, 1)}pp",
            showarrow=False, font=dict(size=14, color="#333"), yref="paper",
        )
        fig.update_layout(
            height=200, margin=dict(l=0, r=60, t=10, b=0),
            xaxis=dict(range=[0, otif_potencial * 1.25], showgrid=False, showticklabels=False),
            yaxis=dict(showgrid=False),
            plot_bgcolor="white", paper_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    with st.expander("Por que a fracao de pedidos afetados por stockout e maior do que a fracao de dias com stockout?"):
        st.caption(
            "A abordagem anterior juntava cada pedido ao estoque do seu order_date: se naquele dia especifico havia stockout, o pedido era marcado como 'afetado'. "
            "Resultado: uma fracao pequena dos pedidos era capturada, com ganho marginal no OTIF.\n\n"
            "O problema: o lead time real e de varios dias. Um stockout que comeca apos o pedido nao e capturado por esse join, e possivelmente contribui para atrasos na entrega.\n\n"
            "**Exemplo:** pedido feito em 01/Jul, entregue em 10/Jul (EU, Produto C).\n\n"
            "```\n"
            "Data      stock_level   stockout\n"
            "01/Jul        450         Nao   <- order_date: join antigo para aqui\n"
            "02/Jul        380         Nao\n"
            "03/Jul        120         Nao\n"
            "04/Jul          0         SIM  <- estoque acaba\n"
            "05/Jul          0         SIM\n"
            "06/Jul          0         SIM\n"
            "07/Jul          0         SIM  <- pedido fica parado\n"
            "08/Jul        200         Nao   <- reabastecimento\n"
            "10/Jul        410         Nao   <- entrega com 4 dias de atraso\n"
            "```\n\n"
            "O join antigo classifica esse pedido como 'sem stockout'. O range join (order_date ate actual_delivery_date) detecta os dias de ruptura no meio do ciclo.\n\n"
            "Com uma janela de entrega de varios dias e stockout ocorrendo em uma parcela significativa do calendario, a probabilidade de algum dia nessa janela coincidir com stockout e maior do que a probabilidade do dia exato do pedido estar em stockout. "
            "Isso possivelmente explica a diferenca observada entre as duas abordagens, embora a relacao de causalidade precise de validacao operacional."
        )


# ---------------------------------------------------------------------------
# Tab 4 , O que fazer e quando?
# ---------------------------------------------------------------------------

def tab_recomendacoes(df: pd.DataFrame):
    st.subheader("O que fazer e quando?")
    st.caption(
        "Prioridades revisadas com base nos achados quantitativos: o desalinhamento entre o SLA contratado e o lead time real e o principal driver do OTIF baixo, "
        "afetando a maioria dos pedidos. A redistribuicao de estoque, se as premissas de causalidade estiverem corretas, possivelmente e executavel sem aumento de producao e com impacto estimado no OTIF global. "
        "A ausencia de data de entrega em uma parcela dos registros significa que o problema pode ser ainda maior do que o medido."
    )

    st.markdown(
        "| Prioridade | Acao | Impacto | Quando atuar |\n"
        "|---|---|---|---|\n"
        "| Alta | Revisar SLA | O lead time real e sistematicamente superior ao SLA contratado. A maioria dos pedidos chega apos a data prometida. Realinhar o SLA com a capacidade operacional real possivelmente seria a acao de maior impacto imediato no OTIF. | Imediato |\n"
        "| Alta | Implementar status_pedido no sistema transacional | Uma parcela dos pedidos nao tem data de entrega registrada, tornando o OTIF impreciso. O problema real pode ser maior do que o medido. | Imediato |\n"
        "| Media | Criar plano de redistribuicao de estoque entre regioes por produto | Ha regioes com excesso e outras em ruptura para o mesmo produto no mesmo periodo. Capital possivelmente imobilizado na regiao errada. Resolver o desequilibrio possivelmente elevaria o OTIF sem necessidade de aumentar producao, sujeito a validacao operacional. | Antes do periodo critico |\n"
        "| Media | Criar politica de reabastecimento diferenciada por produto e regiao | Cada produto e regiao apresenta perfil de stockout distinto por periodo. Uma politica unica de reposicao possivelmente subaproveita a previsibilidade disponivel nos dados historicos. | Q2 |\n"
        "| Baixa | Implementar visibilidade compartilhada de estoque entre regioes | Ha indicio de operacao descoordenada entre regioes. Visibilidade compartilhada permitiria redistribuicao proativa antes da ruptura. | Q2-Q3 |\n"
        "| Baixa | Auditar registros de estoque negativo e capacidade de producao | Registros com valores inconsistentes possivelmente distorcem analises de overflow, correlacao e projecao de capacidade. | Q4 |\n"
    )

    st.markdown("---")
    with st.expander("Por que esse grafico?", expanded=True):
        st.caption(
            "Motivacao: sequenciar as acoes no tempo considerando janelas operacionais criticas. "
            "Os dados sugerem que acoes de curto prazo deveriam ser executadas antes do periodo critico, que parece ser sistematicamente o de maior pressao sobre o estoque."
        )
    st.markdown("**Cronograma de acoes ao longo do ano**")

    gantt_df = pd.DataFrame([
        dict(Acao="1. Revisar SLA",                 Inicio="2025-01-01", Fim="2025-03-01"),
        dict(Acao="2. Implementar status_pedido",    Inicio="2025-01-01", Fim="2025-04-01"),
        dict(Acao="3. Redistribuir estoque",         Inicio="2025-02-01", Fim="2025-05-01"),
        dict(Acao="4. Politica de reabastecimento",  Inicio="2025-03-01", Fim="2025-07-01"),
        dict(Acao="5. Visibilidade compartilhada",   Inicio="2025-04-01", Fim="2025-09-01"),
        dict(Acao="6. Auditar dados",                Inicio="2025-10-01", Fim="2025-12-31"),
    ])
    gantt_df["Inicio"] = pd.to_datetime(gantt_df["Inicio"])
    gantt_df["Fim"]    = pd.to_datetime(gantt_df["Fim"])

    fig = px.timeline(
        gantt_df, x_start="Inicio", x_end="Fim", y="Acao",
        color="Acao", color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        height=320, margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        xaxis=dict(showgrid=False, title=""),
        yaxis=dict(showgrid=False, title=""),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)

    with st.expander("Como o SLA de 5 dias foi obtido", expanded=True):
        st.caption(
            "O SLA contratado e confirmado diretamente nos dados: todos os pedidos tem requested_delivery_date = order_date + SLA. "
            "O lead time real medio (order_date ate actual_delivery_date) e superior ao SLA, "
            "gerando um atraso medio alem do prazo prometido. "
            "A recomendacao de revisar o SLA alinha o contrato com a capacidade operacional real "
            "enquanto as causas raiz do lead time elevado sao endereçadas."
        )

    st.markdown("**OTIF simulado por SLA, impacto de flexibilizar o contrato**")
    d = df[df["total_pedidos"] > 0].copy()
    sla_vals = [5, 6, 7, 8, 10]
    otif_por_sla = []
    for sla in sla_vals:
        extra    = d[d["atraso_medio"] <= (sla - 5)]["pedidos_com_atraso"].sum()
        otif_sim = round(100 * (d["pedidos_on_time"].sum() + extra) / d["total_pedidos"].sum(), 1)
        otif_por_sla.append(otif_sim)

    fig = go.Figure(go.Bar(
        x=[f"{s}d" for s in sla_vals],
        y=otif_por_sla,
        marker_color=[RED if s == 5 else BLUE for s in sla_vals],
        text=[f"{v}%" for v in otif_por_sla],
        textposition="outside",
    ))
    fig.add_annotation(
        x="5d", y=otif_por_sla[0],
        text="SLA atual",
        showarrow=True, arrowhead=2, ax=0, ay=-30,
        font=dict(size=11, color=RED),
    )
    fig.update_layout(
        height=280, margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(range=[0, max(otif_por_sla) * 1.3], showgrid=False),
        xaxis=dict(title="SLA (dias)"),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True, config=CHART_CONFIG)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.title("Case Ball")
    st.caption(
        "OTIF (On Time In Full) mede a porcentagem de pedidos entregues no prazo e na quantidade correta. "
        "Um OTIF baixo pode indicar falhas sistematicas com impacto no cliente final. "
        "Esta analise concentra o estudo nessa metrica por possivelmente sintetizar falhas de estoque, producao e logistica em um unico numero, "
        "permitindo identificar possiveis causas raiz e recomendar acoes com maior retorno potencial. "
        "Limitacao dos dados: os pedidos registram apenas quantidade solicitada, sem quantidade entregue. "
    )

    df = carregar_dados()
    regioes, produtos, inicio, fim, granularidade = renderizar_sidebar(df)

    if not regioes or not produtos:
        st.warning("Selecione ao menos uma regiao e um produto.")
        return

    df_f = aplicar_filtros(df, regioes, produtos, inicio, fim)

    if df_f.empty:
        st.warning("Nenhum dado para os filtros selecionados.")
        return

    tab1, tab2, tab3, tab4 = st.tabs([
        "Qual e o problema?",
        "O estoque esta no lugar errado?",
        "O que causa os atrasos graves?",
        "O que fazer e quando?",
    ])

    with tab1:
        tab_problema(df_f, granularidade)
    with tab2:
        tab_estoque(df_f, granularidade)
    with tab3:
        tab_causas(df_f, granularidade)
    with tab4:
        tab_recomendacoes(df_f)

    st.caption("Fonte: data/gold/obt.parquet | Jan 2023 – Dez 2024 | 8.772 registros")


if __name__ == "__main__":
    main()
