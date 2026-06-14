"""Dashboard: gráficos, projeções e KPIs."""
from dateutil.relativedelta import relativedelta
import datetime
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from helpers import formatar_moeda, logger
from db import consultar_dados

def gerar_meses_futuros(data_inicio, n_meses):
    """Gera uma lista de objetos datetime.date para os n meses futuros."""
    datas = []
    for i in range(n_meses):
        datas.append(data_inicio + relativedelta(months=i))
    return datas

def criar_grafico_saldo_combinado(df_saldo, titulo):
    # Pivotar de volta para o formato largo para facilitar a plotagem separada
    df_pivot = df_saldo.pivot_table(
        index='ano_mes',
        columns='Tipo',
        values='Valor'
    ).fillna(0).reset_index()

    # Garantir que a ordem dos meses esteja correta
    meses_ordenados = sorted(df_pivot['ano_mes'].unique())
    df_pivot = df_pivot.set_index('ano_mes').reindex(meses_ordenados).reset_index()

    fig = go.Figure()

    # 1. Barras de Receita
    fig.add_trace(go.Bar(
        x=df_pivot['ano_mes'],
        y=df_pivot['Receita'],
        name='Receita',
        marker_color='#4BBF7C' # Verde
    ))

    # 2. Barras de Despesa
    fig.add_trace(go.Bar(
        x=df_pivot['ano_mes'],
        y=df_pivot['Despesa'],
        name='Despesa',
        marker_color='#FF6347' # Vermelho
    ))

    # 3. Linha de Saldo
    fig.add_trace(go.Scatter(
        x=df_pivot['ano_mes'],
        y=df_pivot['Saldo_Mensal'],
        name='Saldo',
        mode='lines+markers',
        line=dict(color='#4682B4', width=3), # Azul
        marker=dict(size=8),
        yaxis='y2' # CRÍTICO: Usa um eixo Y secundário para o Saldo
    ))

    # Configurações de Layout
    fig.update_layout(
        title=titulo,
        # Eixo Y primário (Receita/Despesa)
        yaxis=dict(
            title='Receita / Despesa',
            tickformat=".2f"
        ),
        # Eixo Y secundário (Saldo)
        yaxis2=dict(
            title='Saldo',
            overlaying='y',
            side='right',
            tickformat=".2f"
        ),
        barmode='group',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis_title='Mês/Ano'
    )

    return fig

def projetar_dados_futuro(df_passado, df_futuro_agregado, meses_futuro_ref):
    # 1. Calcular as Médias Mensais Históricas (dos últimos 13 meses)

    # Filtrar Receita e Despesa do passado para calcular a média
    df_receita_despesa_passada = df_passado[
        df_passado['Tipo'].isin(['Receita', 'Despesa'])
    ].copy()

    media_mensal = df_receita_despesa_passada.groupby('Tipo')['Valor'].mean().reset_index()
    media_receita = media_mensal[media_mensal['Tipo'] == 'Receita']['Valor'].iloc[0] if 'Receita' in media_mensal['Tipo'].values else 0
    media_despesa = media_mensal[media_mensal['Tipo'] == 'Despesa']['Valor'].iloc[0] if 'Despesa' in media_mensal['Tipo'].values else 0

    # 2. Criar o DataFrame de Projeção
    df_projecao = pd.DataFrame({'ano_mes': meses_futuro_ref})

    # Preenche inicialmente com as médias históricas
    df_projecao['Receita_Projetada'] = media_receita
    df_projecao['Despesa_Projetada'] = media_despesa

    # 3. Incorporar dados já AGENDADOS
    df_futuro_pivot = df_futuro_agregado.pivot_table(
        index='ano_mes',
        columns='Tipo',
        values='Valor',
        aggfunc='sum'
    ).fillna(0)

    # Combina Receita e Receita (Salário) nos dados futuros
    df_futuro_pivot['Receita_Agendada'] = df_futuro_pivot.get('Receita', 0) + df_futuro_pivot.get('Receita (Salário)', 0)
    df_futuro_pivot['Despesa_Agendada'] = df_futuro_pivot.get('Despesa', 0)

    # Juntar os dados agendados com a projeção
    df_projecao = df_projecao.set_index('ano_mes').join(
        df_futuro_pivot[['Receita_Agendada', 'Despesa_Agendada']], 
        how='left'
    ).reset_index().fillna(0)

    # 4. Regra de Projeção Final: Se Agendado > 0, usar Agendado; senão, usar Projetado.
    df_projecao['Receita'] = np.where(
        df_projecao['Receita_Agendada'] > 0, 
        df_projecao['Receita_Agendada'], 
        df_projecao['Receita_Projetada']
    )

    df_projecao['Despesa'] = np.where(
        df_projecao['Despesa_Agendada'] > 0, 
        df_projecao['Despesa_Agendada'], 
        df_projecao['Despesa_Projetada']
    )

    # 5. Calcular o Saldo Final
    df_projecao['Saldo_Mensal'] = df_projecao['Receita'] - df_projecao['Despesa']

    # 6. Transformar para o formato longo (para plotly)
    df_saldo_futuro_final = pd.melt(
        df_projecao,
        id_vars=['ano_mes'],
        value_vars=['Receita', 'Despesa', 'Saldo_Mensal'],
        var_name='Tipo',
        value_name='Valor'
    )

    return df_saldo_futuro_final

def dashboard():
    st.title("📊 Dashboard Financeiro")

    # 1. CONSULTA DE DADOS
    try:
        df_transacoes = consultar_dados("stg_transacoes") 
        df_salario = consultar_dados("fact_salario")

    except Exception as e:
        logger.exception("Erro ao carregar dados de transação/salário no dashboard")
        st.warning(f"Não foi possível carregar os dados de transação/salário. Verifique as tabelas. Erro: {e}")
        return

    if df_transacoes.empty and df_salario.empty:
        st.info("Nenhuma transação ou salário encontrado para gerar o dashboard.")
        return

    # --- PRÉ-PROCESSAMENTO GERAL ---

    # 1. Preparar df_transacoes (Receitas agendadas e Despesas)
    if not df_transacoes.empty:
        df_transacoes['dt_datatransacao'] = pd.to_datetime(df_transacoes['dt_datatransacao'])

        df_transacoes_tipo = df_transacoes.groupby(['dt_datatransacao', 'dsc_tipotransacao'])['vl_transacao'].sum().reset_index()
        df_transacoes_tipo = df_transacoes_tipo.rename(columns={'vl_transacao': 'Valor'})
        df_transacoes_tipo['ano_mes'] = df_transacoes_tipo['dt_datatransacao'].dt.to_period('M').astype(str)

        df_transacoes_tipo = df_transacoes_tipo[df_transacoes_tipo['dsc_tipotransacao'].isin(['Receita', 'Despesas'])].copy()
        df_transacoes_tipo['Tipo'] = df_transacoes_tipo['dsc_tipotransacao'].replace({'Despesas': 'Despesa', 'Receita': 'Receita'})

        df_transacoes_tipo = df_transacoes_tipo[['ano_mes', 'Tipo', 'Valor']]
    else:
        df_transacoes_tipo = pd.DataFrame(columns=['ano_mes', 'Tipo', 'Valor'])

    # 2. Preparar df_salario (Receitas)
    if not df_salario.empty:
        df_salario['dt_recebimento'] = pd.to_datetime(df_salario['dt_recebimento'])

        df_salario_agregado = df_salario.groupby(df_salario['dt_recebimento'].dt.to_period('M'))['vl_salario'].sum().reset_index()

        df_salario_agregado['ano_mes'] = df_salario_agregado['dt_recebimento'].astype(str) 
        df_salario_agregado = df_salario_agregado.rename(columns={'vl_salario': 'Valor'})
        df_salario_agregado['Tipo'] = 'Receita (Salário)'

        df_salario_final = df_salario_agregado[['ano_mes', 'Tipo', 'Valor']].copy()
    else:
        df_salario_final = pd.DataFrame(columns=['ano_mes', 'Tipo', 'Valor'])


    # 3. UNIR DADOS (Salário + Transações)
    df_dados_mensais = pd.concat([
        df_transacoes_tipo,
        df_salario_final
    ])

    df_dados_mensais = df_dados_mensais.groupby(['ano_mes', 'Tipo'])['Valor'].sum().reset_index()


    PALETA_CORES = px.colors.qualitative.Plotly

    today = datetime.date.today()

    # -----------------------------------------------------------------
    # KPIs — mês atual
    # -----------------------------------------------------------------
    mes_atual_str = today.strftime('%Y-%m')
    df_kpi = df_dados_mensais[df_dados_mensais['ano_mes'] == mes_atual_str]
    if not df_kpi.empty:
        df_kpi_pivot = df_kpi.pivot_table(index='ano_mes', columns='Tipo', values='Valor', aggfunc='sum').fillna(0)
        receita_kpi = float(df_kpi_pivot['Receita'].sum() if 'Receita' in df_kpi_pivot.columns else 0)
        receita_kpi += float(df_kpi_pivot['Receita (Salário)'].sum() if 'Receita (Salário)' in df_kpi_pivot.columns else 0)
        despesa_kpi = float(df_kpi_pivot['Despesa'].sum() if 'Despesa' in df_kpi_pivot.columns else 0)
    else:
        receita_kpi, despesa_kpi = 0.0, 0.0
    saldo_kpi = receita_kpi - despesa_kpi

    col_k1, col_k2, col_k3 = st.columns(3)
    col_k1.metric("Receitas (mês atual)", f"R$ {formatar_moeda(receita_kpi)}")
    col_k2.metric("Despesas (mês atual)", f"R$ {formatar_moeda(despesa_kpi)}")
    col_k3.metric("Saldo (mês atual)", f"R$ {formatar_moeda(saldo_kpi)}", delta=round(saldo_kpi, 2))

    st.markdown("---")

    # -----------------------------------------------------------------
    # FILTRO DE TEMPO
    # -----------------------------------------------------------------
    with st.expander("⚙️ Configurar período de análise", expanded=False):
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            n_meses_passado = st.slider("Meses no passado", min_value=1, max_value=36, value=13, key="dash_meses_passado")
        with col_f2:
            n_meses_futuro = st.slider("Meses no futuro", min_value=1, max_value=24, value=12, key="dash_meses_futuro")

    # 1. VISÃO PASSADA
    start_date_passado = today.replace(day=1) - relativedelta(months=n_meses_passado - 1)
    end_limit_passado = today.replace(day=1) + relativedelta(months=1)

    meses_passado = [
        (today.replace(day=1) - relativedelta(months=i)).strftime('%Y-%m')
        for i in range(n_meses_passado - 1, -1, -1)
    ]

    df_passado_saldo = df_dados_mensais[df_dados_mensais['ano_mes'].isin(meses_passado)].copy()

    # 2. VISÃO FUTURA
    start_date_futuro = today.replace(day=1) + relativedelta(months=1)
    end_date_futuro = start_date_futuro + relativedelta(months=n_meses_futuro)

    meses_futuro = [
        (start_date_futuro + relativedelta(months=i)).strftime('%Y-%m')
        for i in range(n_meses_futuro)
    ]

    df_futuro_saldo = df_dados_mensais[df_dados_mensais['ano_mes'].isin(meses_futuro)].copy()


    # -----------------------------------------------------------------
    # GERAÇÃO DO DATAFRAME DE SALDO (Passado e Futuro)
    # -----------------------------------------------------------------
    def gerar_df_saldo(df, meses_ref):
        if df.empty: return pd.DataFrame()
        df_pivot = df.pivot_table(index='ano_mes', columns='Tipo', values='Valor', aggfunc='sum').fillna(0)
        df_pivot['Receita'] = df_pivot.get('Receita', 0) + df_pivot.get('Receita (Salário)', 0)
        df_pivot['Despesa'] = df_pivot.get('Despesa', 0) 
        df_pivot['Saldo_Mensal'] = df_pivot['Receita'] - df_pivot['Despesa']
        df_completo = pd.DataFrame({'ano_mes': meses_ref}).set_index('ano_mes')
        df_pivot = df_completo.join(df_pivot, how='left').fillna(0).reset_index()
        df_saldo_longo = pd.melt(df_pivot, id_vars=['ano_mes'], value_vars=['Receita', 'Despesa', 'Saldo_Mensal'], var_name='Tipo', value_name='Valor')
        return df_saldo_longo

    df_saldo_passado_final = gerar_df_saldo(df_passado_saldo, meses_ref=sorted(meses_passado))
    df_saldo_futuro_final = projetar_dados_futuro(df_passado_saldo, df_futuro_saldo, meses_futuro_ref=sorted(meses_futuro))


    # -----------------------------------------------------------------
    # PRIMEIRA LINHA DE GRÁFICOS (SALDO)
    # -----------------------------------------------------------------
    col_saldo_passado, col_saldo_futuro = st.columns(2)

    with col_saldo_passado:
        st.subheader("Balanço Mensal (Passado)")
        if not df_saldo_passado_final.empty:
            fig3 = criar_grafico_saldo_combinado(df_saldo_passado_final, f'Receitas, Despesas e Saldo (Últimos {n_meses_passado} Meses)')
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Dados de balanço insuficientes no período passado.")

    with col_saldo_futuro:
        st.subheader("Projeção de Balanço (Futuro)")
        if not df_saldo_futuro_final.empty:
            fig4 = criar_grafico_saldo_combinado(df_saldo_futuro_final, f'Projeção de Balanço (Próximos {n_meses_futuro} Meses)')
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Nenhuma projeção de transação disponível para o período futuro.")

    # -----------------------------------------------------------
    # SEGUNDA LINHA DE GRÁFICOS (Evolução por Categoria + Acumulado)
    # -----------------------------------------------------------
    st.markdown("---") 

    col_grafico1, col_grafico2, col_grafico5 = st.columns(3)

    # -----------------------------------------------------------------
    # Gráfico 1: Evolução Mensal por Categoria (Passado) - Coluna 1
    # -----------------------------------------------------------------
    with col_grafico1:
        st.subheader("Evolução Mensal por Categoria")

        df_passado_categoria = df_transacoes[
            (df_transacoes['dt_datatransacao'].dt.date >= start_date_passado) &
            (df_transacoes['dt_datatransacao'].dt.date < end_limit_passado)
        ].copy()

        if not df_passado_categoria.empty:
            df_passado_categoria['ano_mes'] = df_passado_categoria['dt_datatransacao'].dt.to_period('M').astype(str)
            df_agregado_mensal = df_passado_categoria.groupby(['ano_mes', 'dsc_categoriatransacao'])['vl_transacao'].sum().reset_index()

            meses_ordenados = sorted(df_agregado_mensal['ano_mes'].unique())
            categoria_ordenada = df_agregado_mensal.groupby('dsc_categoriatransacao')['vl_transacao'].sum().sort_values(ascending=False).index.tolist()

            fig1 = px.bar(
                df_agregado_mensal,
                x='ano_mes',
                y='vl_transacao',
                color='dsc_categoriatransacao',
                title=f'Passado (Últimos {n_meses_passado} Meses)',
                labels={'ano_mes': 'Mês/Ano', 'vl_transacao': 'Valor Total'},
                category_orders={"ano_mes": meses_ordenados, "dsc_categoriatransacao": categoria_ordenada},
                color_discrete_sequence=PALETA_CORES 
            )
            fig1.update_layout(xaxis_title='Mês/Ano', yaxis_title='Valor', legend_title='Categoria')
            fig1.update_yaxes(tickformat=".2f") 
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Dados insuficientes nos últimos 13 meses.")

    # -----------------------------------------------------------------
    # Gráfico 2: Transações por Categoria (Futuro) - Coluna 2
    # -----------------------------------------------------------------
    with col_grafico2:
        st.subheader("Transações Agendadas por Categoria")

        df_futuro_categoria = df_transacoes[
            (df_transacoes['dt_datatransacao'].dt.date >= start_date_futuro) &
            (df_transacoes['dt_datatransacao'].dt.date < end_date_futuro)
        ].copy()

        if not df_futuro_categoria.empty:
            df_futuro_categoria['ano_mes'] = df_futuro_categoria['dt_datatransacao'].dt.to_period('M').astype(str)
            df_agregado_futuro = df_futuro_categoria.groupby(['ano_mes', 'dsc_categoriatransacao'])['vl_transacao'].sum().reset_index()

            meses_futuros_ordenados = sorted(df_agregado_futuro['ano_mes'].unique())
            categoria_futura_ordenada = df_agregado_futuro.groupby('dsc_categoriatransacao')['vl_transacao'].sum().sort_values(ascending=False).index.tolist()

            fig2 = px.bar(
                df_agregado_futuro,
                x='ano_mes',
                y='vl_transacao',
                color='dsc_categoriatransacao',
                title=f'Futuro (Próximos {n_meses_futuro} Meses)',
                labels={'ano_mes': 'Mês/Ano', 'vl_transacao': 'Valor Total'},
                category_orders={"ano_mes": meses_futuros_ordenados, "dsc_categoriatransacao": categoria_futura_ordenada},
                color_discrete_sequence=PALETA_CORES 
            )
            fig2.update_layout(xaxis_title='Mês/Ano', yaxis_title='Valor', legend_title='Categoria')
            fig2.update_yaxes(tickformat=".2f")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Nenhuma transação agendada/registrada para o período futuro.")

    # -----------------------------------------------------------------
    # Gráfico 5: Despesas Acumuladas por Ano (Anual) - Coluna 3
    # -----------------------------------------------------------------
    with col_grafico5:
        st.subheader("Despesas Acumuladas por Ano")

        # Filtrar o DataFrame de Transações Apenas para DESPESAS
        df_despesas_acumuladas_anual = df_transacoes[
            df_transacoes['dsc_tipotransacao'] == 'Despesas'
        ].copy()

        if not df_despesas_acumuladas_anual.empty:
            df_despesas_acumuladas_anual['Ano'] = df_despesas_acumuladas_anual['dt_datatransacao'].dt.year

            df_agregado_anual = df_despesas_acumuladas_anual.groupby(['Ano', 'dsc_categoriatransacao'])['vl_transacao'].sum().reset_index()
            df_agregado_anual['Ano'] = df_agregado_anual['Ano'].astype(str)

            # Ajuste de Ordenação da Pilha (Cores)
            categoria_ordenada_acumulada = df_agregado_anual.groupby('dsc_categoriatransacao')['vl_transacao'].sum().sort_values(ascending=False).index.tolist()

            fig5 = px.bar(
                df_agregado_anual,
                x='Ano',
                y='vl_transacao',
                color='dsc_categoriatransacao',
                barmode='stack',
                title='Distribuição de Despesas por Categoria (Acumulado Anual)',
                labels={'vl_transacao': 'Valor Acumulado (R$)', 'dsc_categoriatransacao': 'Categoria'},
                color_discrete_sequence=PALETA_CORES, 
                category_orders={"dsc_categoriatransacao": categoria_ordenada_acumulada}
            )

            # Ordenação do Eixo X (Ano) é mantida cronológica (2023, 2024, 2025)
            fig5.update_layout(
                xaxis_title='Ano', 
                yaxis_title='Valor Acumulado',
                legend_title='Categoria'
            )
            fig5.update_yaxes(tickformat=".2f")
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.info("Nenhuma despesa registrada para o cálculo acumulado por ano.")

    # -----------------------------------------------------------
    # TERCEIRA LINHA DE GRÁFICOS (Evolução por Subcategoria + Acumulado)
    # -----------------------------------------------------------
    st.markdown("---") 

    col_grafico6, col_grafico7, col_grafico8 = st.columns(3)

    # -----------------------------------------------------------------
    # Gráfico 6: Evolução Mensal por Subcategoria (Passado) - Coluna 1
    # -----------------------------------------------------------------
    with col_grafico6:
        st.subheader("Evolução Mensal por Subcategoria")

        df_passado_subcategoria = df_transacoes[
            (df_transacoes['dt_datatransacao'].dt.date >= start_date_passado) &
            (df_transacoes['dt_datatransacao'].dt.date < end_limit_passado)
        ].copy()

        if not df_passado_subcategoria.empty:
            df_passado_subcategoria['ano_mes'] = df_passado_subcategoria['dt_datatransacao'].dt.to_period('M').astype(str)
            # Agrupar por Mês e Subcategoria
            df_agregado_mensal_sub = df_passado_subcategoria.groupby(['ano_mes', 'dsc_subcategoriatransacao'])['vl_transacao'].sum().reset_index()

            meses_ordenados = sorted(df_agregado_mensal_sub['ano_mes'].unique())
            # Ordenar subcategorias para consistência de cores
            subcategoria_ordenada = df_agregado_mensal_sub.groupby('dsc_subcategoriatransacao')['vl_transacao'].sum().sort_values(ascending=False).index.tolist()

            fig6 = px.bar(
                df_agregado_mensal_sub,
                x='ano_mes',
                y='vl_transacao',
                color='dsc_subcategoriatransacao',
                title=f'Passado (Últimos {n_meses_passado} Meses)',
                labels={'ano_mes': 'Mês/Ano', 'vl_transacao': 'Valor Total'},
                category_orders={"ano_mes": meses_ordenados, "dsc_subcategoriatransacao": subcategoria_ordenada},
                color_discrete_sequence=PALETA_CORES 
            )
            fig6.update_layout(xaxis_title='Mês/Ano', yaxis_title='Valor', legend_title='Subcategoria')
            fig6.update_yaxes(tickformat=".2f") 
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info("Dados insuficientes de subcategorias no período passado.")

    # -----------------------------------------------------------------
    # Gráfico 7: Transações por Subcategoria (Futuro) - Coluna 2
    # -----------------------------------------------------------------
    with col_grafico7:
        st.subheader("Transações Agendadas por Subcategoria")

        df_futuro_subcategoria = df_transacoes[
            (df_transacoes['dt_datatransacao'].dt.date >= start_date_futuro) &
            (df_transacoes['dt_datatransacao'].dt.date < end_date_futuro)
        ].copy()

        if not df_futuro_subcategoria.empty:
            df_futuro_subcategoria['ano_mes'] = df_futuro_subcategoria['dt_datatransacao'].dt.to_period('M').astype(str)
            # Agrupar por Mês e Subcategoria
            df_agregado_futuro_sub = df_futuro_subcategoria.groupby(['ano_mes', 'dsc_subcategoriatransacao'])['vl_transacao'].sum().reset_index()

            meses_futuros_ordenados = sorted(df_agregado_futuro_sub['ano_mes'].unique())
            subcategoria_futura_ordenada = df_agregado_futuro_sub.groupby('dsc_subcategoriatransacao')['vl_transacao'].sum().sort_values(ascending=False).index.tolist()

            fig7 = px.bar(
                df_agregado_futuro_sub,
                x='ano_mes',
                y='vl_transacao',
                color='dsc_subcategoriatransacao',
                title=f'Futuro (Próximos {n_meses_futuro} Meses)',
                labels={'ano_mes': 'Mês/Ano', 'vl_transacao': 'Valor Total'},
                category_orders={"ano_mes": meses_futuros_ordenados, "dsc_subcategoriatransacao": subcategoria_futura_ordenada},
                color_discrete_sequence=PALETA_CORES 
            )
            fig7.update_layout(xaxis_title='Mês/Ano', yaxis_title='Valor', legend_title='Subcategoria')
            fig7.update_yaxes(tickformat=".2f")
            st.plotly_chart(fig7, use_container_width=True)
        else:
            st.info("Nenhuma transação agendada/registrada por subcategoria para o período futuro.")

    # -----------------------------------------------------------------
    # Gráfico 8: Despesas Acumuladas por Subcategoria (Anual) - Coluna 3
    # -----------------------------------------------------------------
    with col_grafico8:
        st.subheader("Despesas Acumuladas por Ano (Subcategoria)")

        df_despesas_acumuladas_anual_sub = df_transacoes[
            df_transacoes['dsc_tipotransacao'] == 'Despesas'
        ].copy()

        if not df_despesas_acumuladas_anual_sub.empty:
            df_despesas_acumuladas_anual_sub['Ano'] = df_despesas_acumuladas_anual_sub['dt_datatransacao'].dt.year

            # Agrupar por Ano e Subcategoria
            df_agregado_anual_sub = df_despesas_acumuladas_anual_sub.groupby(['Ano', 'dsc_subcategoriatransacao'])['vl_transacao'].sum().reset_index()
            df_agregado_anual_sub['Ano'] = df_agregado_anual_sub['Ano'].astype(str)

            # Ajuste de Ordenação da Pilha (Cores)
            subcategoria_ordenada_acumulada = df_agregado_anual_sub.groupby('dsc_subcategoriatransacao')['vl_transacao'].sum().sort_values(ascending=False).index.tolist()

            # Utilizar uma paleta maior, pois há muitas subcategorias (Dark24 é bom para isso)
            fig8 = px.bar(
                df_agregado_anual_sub,
                x='Ano',
                y='vl_transacao',
                color='dsc_subcategoriatransacao',
                barmode='stack',
                title='Distribuição de Despesas por Subcategoria (Anual)',
                labels={'vl_transacao': 'Valor Acumulado (R$)', 'dsc_subcategoriatransacao': 'Subcategoria'},
                color_discrete_sequence=px.colors.qualitative.Dark24, # Paleta expandida
                category_orders={"dsc_subcategoriatransacao": subcategoria_ordenada_acumulada}
            )

            fig8.update_layout(
                xaxis_title='Ano', 
                yaxis_title='Valor Acumulado',
                legend_title='Subcategoria',
                legend=dict(font=dict(size=10)) # Reduz o tamanho da legenda devido ao número de itens
            )
            fig8.update_yaxes(tickformat=".2f")
            st.plotly_chart(fig8, use_container_width=True)
        else:
            st.info("Nenhuma despesa registrada por subcategoria para o cálculo acumulado por ano.")
