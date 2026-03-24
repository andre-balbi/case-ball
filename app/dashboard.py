from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).parent.parent
OBT  = ROOT / "data" / "gold" / "obt.parquet"
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


def aplicar_filtros(df, regioes, produtos, inicio, fim):
    mask = (
        df["region"].isin(regioes)
        & df["product"].isin(produtos)
        & (df["date"] >= inicio)
        & (df["date"] <= fim)
    )
    return df[mask].copy()


def renderizar_sidebar(df):
    st.sidebar.image(str(ROOT / "figs" / "logo-ball.jpg"), use_container_width=True)
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
        "Com OTIF de 17,5%, apenas 1 em cada 6 pedidos e entregue no prazo e na quantidade correta. "
        "Os graficos abaixo mostram quando o problema ocorre e quais produtos e regioes sao mais afetados."
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
    atraso  = round(d["atraso_medio"].mean(), 1)

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
        ("Atraso medio (dias)", f"{atraso}", None, "off"),
        ("OTIF Q3 vs media", f"{q3_otif}%", f"{q3_otif - otif_geral:+.1f}pp", "inverse"),
        (f"Pior produto: {pior_prod}", f"{pior_valor}%", None, "off"),
    ])

    st.markdown("---")

    # Linha 1: evolucao temporal + OTIF por produto ordenado
    with st.expander("Por que esses graficos?", expanded=True):
        st.caption(
            "Motivacao: verificar se o OTIF baixo e um problema pontual ou estrutural. "
            "Conclusao: o padrao se repete em todos os periodos, com Q3 sistematicamente abaixo da media , "
            "o problema e estrutural e previsivel, nao um evento isolado."
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
            "Conclusao: Produto C em EU concentra o pior desempenho , ha heterogeneidade que permite priorizar acoes especificas."
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
    unid_paradas = int(deseq["stock_level"].sum())

    stockout_by_region = df.groupby("region")["stockout_flag"].sum()
    pior_so_regiao = stockout_by_region.idxmax() if not stockout_by_region.empty else "-"
    dias_so_pior = int(stockout_by_region.max()) if not stockout_by_region.empty else 0

    cols = st.columns(3)
    kpi_row(cols, [
        (f"Dias {pior_so_regiao} em stockout", f"{dias_so_pior}", None, "off"),
        ("Dias desequilibrio simultaneo", f"{dias_deseq}", None, "off"),
        ("Unidades paradas (outra regiao)", f"{unid_paradas:,}", None, "off"),
    ])

    st.markdown("---")

    # Stockout e overflow lado a lado por regiao
    with st.expander("Por que esses graficos?", expanded=True):
        st.caption(
            "Motivacao: mapear onde o estoque falha , ruptura ou excesso , por regiao e periodo. "
            "Conclusao: o problema nao e uniforme; algumas regioes sofrem stockout cronico enquanto outras acumulam overflow nos mesmos periodos."
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
            "Conclusao: para o Produto C, LATAM ficou com excesso por ~17 dias enquanto outra regiao estava sem estoque do mesmo item , "
            "o que indica que o problema nao e falta de producao , e falta de redistribuicao entre regioes."
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
            "Conclusao: cada ponto e por definicao um evento simultaneo confirmado. "
            "Produto C entre LATAM e EU acumula mais dias e mais unidades paradas , a redistribuicao de LATAM para EU resolveria a ruptura sem aumentar producao."
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
            "Este grafico e um sinal fraco. A evidencia mais direta de descoordenaacao esta no grafico anterior: mesmo produto em stockout em uma regiao e overflow em outra no mesmo dia."
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
        "Se stockout e atraso co-ocorrem nos mesmos periodos, ha evidencia de causalidade temporal. "
        "Os graficos abaixo testam essa hipotese: quando o estoque acaba em uma regiao, os pedidos atrasam. "
        "Resolver o desequilibrio de distribuicao elevaria o OTIF de forma direta e mensuravel."
    )

    d = df[df["total_pedidos"] > 0]
    pct_atraso = round(100 * d["pedidos_com_atraso"].sum() / d["total_pedidos"].sum(), 1)
    pct_stockout = round(100 * df["stockout_flag"].mean(), 1)
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

    st.markdown("---")

    # Dual-axis: stockout + atraso por periodo
    label = "trimestre" if granularidade == "Trimestral" else "mes"
    with st.expander("O que esse grafico me fornece?", expanded=True):
        st.caption(
            "Este grafico sobrepoe dois indicadores no mesmo eixo de tempo: a porcentagem de dias com stockout (barras, eixo esquerdo) "
            "e o atraso medio de entrega em dias (linha, eixo direito). "
            "Se os dois sobem e descem juntos nos mesmos periodos, ha evidencia temporal de que a ruptura de estoque precede ou coincide com os atrasos , "
            "o que sugere relacao de causa e efeito. Nao prova causalidade direta , mas e o tipo de padrao que justifica investigar stockout como causa raiz antes de outras hipoteses."
        )
    st.markdown(f"**Stockout e atraso co-ocorrem por {label} , evidencia de causalidade**")

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
            "Permite identificar qual produto tem mais rupturas e se o problema e constante ou concentrado em periodos especificos. "
            "Grafico da direita: simula o OTIF total se os pedidos dos dias com stockout tivessem a mesma taxa de cumprimento dos dias sem stockout. "
            "Resultado esperado: o ganho e pequeno (~0.2pp) porque stockout afeta apenas ~4% dos pedidos em volume. "
            "Isso revela que stockout NAO e a causa primaria do OTIF baixo: mesmo nos dias sem ruptura o OTIF e de apenas 17.7%. "
            "O principal driver e o desalinhamento entre o SLA contratado (5 dias) e o atraso medio operacional (3.7 dias) , que classifica como falha a maioria dos pedidos independente de stockout. Ver Tab 4 para simulacao de SLA."
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

    # OTIF atual vs potencial
    with col2:
        st.markdown("**OTIF atual vs potencial sem stockout**")
        otif_atual = round(100 * d["pedidos_on_time"].sum() / d["total_pedidos"].sum(), 1)

        # Simulacao: pedidos dos dias com stockout passam a ter a mesma taxa dos dias sem stockout
        d_so  = d[d["stockout_flag"]]
        d_nso = d[~d["stockout_flag"]]
        otif_ns_rate = d_nso["pedidos_on_time"].sum() / d_nso["total_pedidos"].sum() if d_nso["total_pedidos"].sum() > 0 else 0
        pedidos_on_time_pot = d_nso["pedidos_on_time"].sum() + d_so["total_pedidos"].sum() * otif_ns_rate
        otif_potencial = round(100 * pedidos_on_time_pot / d["total_pedidos"].sum(), 1)

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


# ---------------------------------------------------------------------------
# Tab 4 , O que fazer e quando?
# ---------------------------------------------------------------------------

def tab_recomendacoes(df: pd.DataFrame):
    st.subheader("O que fazer e quando?")
    st.caption(
        "Prioridades revisadas com base nos achados quantitativos: o SLA desalinhado e o principal driver do OTIF baixo, "
        "afetando 96% dos pedidos. A redistribuicao de estoque e operacionalmente correta mas tem impacto direto de apenas 0.2pp no OTIF. "
        "A ausencia de status_pedido em 4.9% dos registros significa que o problema pode ser ainda maior do que o medido."
    )

    recomendacoes = pd.DataFrame([
        (1, "Alta",
         "Revisar SLA de 5 para 7-8 dias",
         "Principal driver do OTIF baixo: SLA de 5d com atraso medio de 3.7d classifica como falha a maioria dos pedidos. "
         "Nao resolve a operacao, mas alinha o contrato com a realidade enquanto as causas raiz sao tratadas.",
         "Imediato"),
        (2, "Alta",
         "Implementar status_pedido no sistema transacional",
         "306 pedidos (4.9%) sem data de entrega tornam o OTIF impreciso. "
         "O problema real pode ser maior do que os 17.5% medidos.",
         "Imediato"),
        (3, "Media",
         "Criar plano de redistribuicao de estoque entre regioes por produto",
         "Produto C com excesso em LATAM enquanto outras regioes estao em ruptura por 17+ dias. "
         "Impacto direto no OTIF de 0.2pp, mas relevante para reducao de capital imobilizado e nivelamento operacional.",
         "Antes do Q3"),
        (4, "Media",
         "Criar politica de reabastecimento diferenciada por produto e regiao",
         "Produtos A, B e C tem perfis de stockout distintos por periodo. "
         "Politica unica de reposicao subaproveita a previsibilidade disponivel nos dados historicos.",
         "Q2"),
        (5, "Baixa",
         "Implementar visibilidade compartilhada de estoque entre regioes",
         "Correlacoes de stock_level proximas de zero sao um indicio (nao prova) de operacao descoordenada. "
         "Visibilidade compartilhada permitiria redistribuicao proativa antes da ruptura.",
         "Q2-Q3"),
        (6, "Baixa",
         "Auditar registros de estoque negativo e producao_capacity",
         "336 dias com stock_level negativo distorcem analises de overflow e correlacao. "
         "Inconsistencias em producao_capacity dificultam projecoes de capacidade.",
         "Q4"),
    ], columns=["#", "Prioridade", "Acao", "Impacto", "Quando atuar"])

    st.dataframe(
        recomendacoes, use_container_width=True, hide_index=True,
        column_config={
            "#": st.column_config.NumberColumn(width="small"),
            "Prioridade": st.column_config.TextColumn(width="small"),
            "Quando atuar": st.column_config.TextColumn(width="small"),
        },
    )

    st.markdown("---")
    with st.expander("Por que esse grafico?", expanded=True):
        st.caption(
            "Motivacao: sequenciar as acoes no tempo considerando janelas operacionais criticas. "
            "Conclusao: acoes de curto prazo devem ser executadas antes do Q3, que e sistematicamente o pior trimestre e o momento de maior pressao sobre o estoque."
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

    with st.expander("Como o SLA de 5 dias foi inferido", expanded=True):
        st.caption(
            "O OBT nao tem uma coluna sla_dias explicita. O valor de 5 dias foi inferido: "
            "e o unico limiar que torna o OTIF de ~17,5% coerente com um atraso medio de 3,7 dias. "
            "Com SLA de 7 ou 8 dias o OTIF seria muito maior. "
            "A recomendacao de flexibilizar para 7-8 dias nao resolve o problema de distribuicao , "
            "apenas alinha o contrato com a realidade operacional enquanto as causas raiz sao endereçadas. "
            "Limitacao: se o dataset original tiver prazo_contratado explicito, esse valor deve sobrescrever a inferencia."
        )

    st.markdown("**OTIF simulado por SLA , impacto de flexibilizar o contrato**")
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
        "Um OTIF baixo indica falhas sistematicas que afetam diretamente o cliente final. "
        "Com tempo de analise limitado, optei por concentrar o estudo nessa metrica por ser a de maior "
        "impacto visivel, pois ela sintetiza falhas de estoque, producao e logistica em um unico numero, "
        "permitindo identificar causas raiz e recomendar acoes com maior retorno potencial."
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
        "O estoque esta no lugar errado",
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
