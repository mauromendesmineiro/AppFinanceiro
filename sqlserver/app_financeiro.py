import streamlit as st
import pyodbc
import pandas as pd
import datetime

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    layout="wide",  # Define a largura máxima como a largura do navegador
    initial_sidebar_state="auto"
)

# --- CONFIGURAÇÃO DA CONEXÃO (MANTENHA SEU AJUSTE AQUI!) ---
# Lembre-se: Se você instalou o SQLEXPRESS, ajuste o SERVER para o nome correto,
# ex: f'DRIVER={DRIVER};SERVER=NOME_DO_SEU_COMPUTADOR\SQLEXPRESS;...'
SERVER = '(localdb)\\MSSQLLocalDB' 
DATABASE = 'financeiro'
DRIVER = '{ODBC Driver 17 for SQL Server}' 
CONNECTION_STRING = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

# --- FUNÇÃO DE CONSULTA (ATUALIZADA) ---
def get_connection():
    # Esta função agora usa a CONNECTION_STRING definida acima
    conn = pyodbc.connect(CONNECTION_STRING)
    return conn

@st.cache_data(ttl=600)
def consultar_dados(tabela_ou_view, usar_view=False):
    conn = get_connection()
    
    # ----------------------------------------------------
    # Lógica de montagem da query SIMPLIFICADA E CORRIGIDA
    # Agora, tabela_ou_view DEVE conter o prefixo (vw_, dim_, stg_)
    # ----------------------------------------------------
    sql_query = f"SELECT * FROM {tabela_ou_view}" 

    try:
        df = pd.read_sql(sql_query, conn)
        conn.close()
        return df
    except pd.io.sql.DatabaseError as e:
        # Se o erro original não tivesse o 'vw_vw_', usar_view poderia ser usado aqui
        # Mas, para resolver o 'vw_vw_', esta lógica é mais segura.
        st.error(f"Erro ao consultar dados na {tabela_ou_view}: {e}")
        conn.close()
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao conectar ou consultar: {e}")
        if conn: conn.close()
        return pd.DataFrame()


# --- FUNÇÃO DE INSERÇÃO (MANTIDA) ---
def inserir_dados(tabela, dados, campos):
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        cursor = conn.cursor()
        
        colunas = ", ".join(campos)
        placeholders = ", ".join(["?"] * len(campos))
        
        sql_insert = f"INSERT INTO {tabela} ({colunas}) VALUES ({placeholders})"
        
        cursor.execute(sql_insert, *dados)
        conn.commit()
        st.success(f"Dados inseridos com sucesso na tabela {tabela}!")
        
        st.cache_data.clear() # Limpa o cache para atualizar as tabelas
        
    except pyodbc.Error as ex:
        st.error(f"Erro ao inserir dados na {tabela}: {ex}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()


# --- FORMULÁRIOS DE CADASTRO ---

def formulario_tipo_transacao():
    st.header("Cadastro de Tipo de Transação")
    
    with st.form("tipo_form"):
        # Campo de entrada
        descricao = st.text_input("Descrição do Tipo (ex: Receita, Despesa)")
        
        submitted = st.form_submit_button("Inserir Tipo")
        
        if submitted:
            if descricao:
                inserir_dados(
                    tabela="dim_TipoTransacao",
                    dados=(descricao,),
                    campos=("DSC_TipoTransacao",) # Coluna original, pois é para INSERT
                )
            else:
                st.warning("O campo Descrição é obrigatório.")

    # Exibe a tabela usando a View (nomes amigáveis: ID, TipoTransacao)
    st.subheader("Registros Existentes")
    df_tipos = consultar_dados("dim_TipoTransacao")
    st.dataframe(df_tipos, use_container_width=True)


def formulario_categoria():
    st.header("Cadastro de Categoria")
    
    # Consulta o TIPO de Transação (Receita/Despesa) para o Dropdown
    # Usamos a tabela diretamente para garantir as colunas originais (ID_TipoTransacao) para o INSERT
    df_tipos = consultar_dados("dim_TipoTransacao", usar_view=False) 
    
    if df_tipos.empty:
        st.warning("Primeiro, cadastre um Tipo de Transação ('Receita' ou 'Despesa').")
        return

    # Mapeamento do Tipo de Transação (Nome -> ID)
    # A coluna do tipo (original) é DSC_TipoTransacao
    tipos_dict = dict(zip(df_tipos['DSC_TipoTransacao'], df_tipos['ID_TipoTransacao']))
    tipos_nomes = list(tipos_dict.keys())
    
    with st.form("categoria_form"):
        tipo_selecionado_nome = st.selectbox(
            "Selecione o Tipo (Receita/Despesa):",
            tipos_nomes
        )
        descricao = st.text_input("Descrição da Categoria (ex: Alimentação, Investimento)")
        
        submitted = st.form_submit_button("Inserir Categoria")
        
        if submitted and descricao:
            id_tipo = tipos_dict[tipo_selecionado_nome]
            inserir_dados(
                tabela="dim_Categoria",
                dados=(id_tipo, descricao,),
                campos=("ID_TipoTransacao", "DSC_CategoriaTransacao",)
            )

    # Exibe a tabela usando a View (nomes amigáveis: TipoTransacao, Categoria)
    st.subheader("Registros Existentes")
    df_categorias = consultar_dados("dim_Categoria")
    st.dataframe(df_categorias, use_container_width=True)


def formulario_subcategoria():
    st.header("Cadastro de Subcategoria")

    # Consulta a Categoria Pai para o Dropdown
    # Usamos a tabela diretamente para garantir as colunas originais (ID_Categoria) para o INSERT
    df_categorias = consultar_dados("dim_Categoria", usar_view=False)
    
    if df_categorias.empty:
        st.warning("Primeiro, cadastre pelo menos uma Categoria.")
        return

    # Mapeamento da Categoria (Nome -> ID). A coluna original é DSC_CategoriaTransacao
    categorias_dict = dict(zip(df_categorias['DSC_CategoriaTransacao'], df_categorias['ID_Categoria']))
    categorias_nomes = list(categorias_dict.keys())
    
    with st.form("subcategoria_form"):
        categoria_selecionada_nome = st.selectbox(
            "Selecione a Categoria Pai:",
            categorias_nomes
        )
        descricao_subcategoria = st.text_input("Descrição da Subcategoria (ex: Almoço, Supermercado, Aluguel)")
        
        submitted = st.form_submit_button("Inserir Subcategoria")
        
        if submitted:
            if descricao_subcategoria and categoria_selecionada_nome:
                id_categoria = categorias_dict[categoria_selecionada_nome]
                
                inserir_dados(
                    tabela="dim_Subcategoria",
                    dados=(id_categoria, descricao_subcategoria),
                    campos=("ID_Categoria", "DSC_SubcategoriaTransacao")
                )
            else:
                st.warning("Todos os campos são obrigatórios.")

    # Exibe a tabela usando a View (nomes amigáveis: Categoria, Subcategoria)
    st.subheader("Registros Existentes")
    df_subcategorias = consultar_dados("dim_Subcategoria")
    st.dataframe(df_subcategorias, use_container_width=True)


def formulario_usuario():
    st.header("Cadastro de Usuário")
    
    with st.form("usuario_form"):
        # Campo de entrada
        nome = st.text_input("Nome do Usuário:")
        
        submitted = st.form_submit_button("Inserir Usuário")
        
        if submitted:
            if nome:
                inserir_dados(
                    tabela="dim_Usuario",
                    dados=(nome,),
                    campos=("DSC_Nome",) # Coluna original
                )
            else:
                st.warning("O campo Nome é obrigatório.")

    # Exibe a tabela usando a View (nomes amigáveis: ID, Nome)
    st.subheader("Registros Existentes")
    df_usuarios = consultar_dados("dim_Usuario")
    st.dataframe(df_usuarios, use_container_width=True)

def formulario_salario():
    st.header("Registro de Salário")

    # 1. Consulta o Usuário para o Dropdown
    # Usamos a tabela dim_Usuario diretamente (usar_view=False) para obter o ID original
    df_usuarios = consultar_dados("dim_Usuario", usar_view=False)
    
    if df_usuarios.empty:
        st.warning("Primeiro, cadastre pelo menos um Usuário na aba 'Usuário'.")
        return

    # Mapeamento do Usuário (Nome -> ID)
    usuarios_dict = dict(zip(df_usuarios['DSC_Nome'], df_usuarios['ID_Usuario']))
    usuarios_nomes = list(usuarios_dict.keys())
    
    with st.form("salario_form"):
        # Campos do Formulário
        usuario_selecionado_nome = st.selectbox(
            "Selecione o Usuário:",
            usuarios_nomes
        )
        valor_salario = st.number_input("Valor Recebido (Ex: 3500.00)", min_value=0.01, format="%.2f")
        data_recebimento = st.date_input("Data de Recebimento:", datetime.date.today())
        observacao = st.text_area("Observação (Ex: Salário mês X):", max_chars=255)
        
        submitted = st.form_submit_button("Registrar Salário")
        
        if submitted:
            if valor_salario > 0:
                id_usuario = usuarios_dict[usuario_selecionado_nome]
                
                inserir_dados(
                    tabela="fact_Salario", # A nova tabela FATO
                    dados=(id_usuario, valor_salario, data_recebimento, observacao),
                    campos=("ID_Usuario", "VL_Salario", "DT_Recebimento", "DSC_Observacao")
                )
            else:
                st.warning("O Valor do Salário deve ser maior que zero.")

    # Exibe a tabela usando a View (nomes amigáveis: Usuario, Valor, DataRecebimento)
    st.subheader("Salários Registrados")
    df_salarios = consultar_dados("fact_Salario")
    st.dataframe(df_salarios, use_container_width=True)

def reset_categoria():
    """Reseta a Categoria e Subcategoria ao mudar o Tipo de Transação."""
    # Define a chave de Categoria para o primeiro valor (index=0)
    # A Subcategoria é implicitamente reajustada no próximo re-run.
    if 'sel_cat' in st.session_state:
        st.session_state.sel_cat = None
    if 'sel_sub' in st.session_state:
        st.session_state.sel_sub = None

def formulario_transacao():
    st.header("Registro de Transação")
    
    # 1. CARREGAR DADOS DAS DIMENSÕES
    df_tipos = consultar_dados("dim_TipoTransacao", usar_view=False)
    df_categorias = consultar_dados("dim_Categoria", usar_view=False)
    df_subcategorias = consultar_dados("dim_Subcategoria", usar_view=False)
    df_usuarios = consultar_dados("dim_Usuario", usar_view=False)

    if df_tipos.empty or df_categorias.empty or df_subcategorias.empty or df_usuarios.empty:
        st.warning("É necessário cadastrar: Tipos, Categorias, Subcategorias e Usuários.")
        return

    tipos_map = dict(zip(df_tipos['DSC_TipoTransacao'], df_tipos['ID_TipoTransacao']))
    usuarios_map = dict(zip(df_usuarios['DSC_Nome'], df_usuarios['ID_Usuario']))
    
    tipos_nomes = list(tipos_map.keys())
    usuarios_nomes = list(usuarios_map.keys())
    
    # ----------------------------------------------------------------------------------
    # REMOÇÃO DO BLOCO with st.form(...)
    # ----------------------------------------------------------------------------------
    
    # ----------------------------------------
    # LINHA 1: DATA, TIPO E USUÁRIO
    # ----------------------------------------
    col1, col2, col3 = st.columns(3)
    with col1:
        data_transacao = st.date_input("Data da Transação:", datetime.date.today())
    with col2:
        # st.selectbox: Tipo de Transação - COM CALLBACK (Funciona fora do st.form)
        tipo_nome = st.selectbox(
            "Tipo de Transação:", 
            tipos_nomes, 
            key="sel_tipo", 
            on_change=reset_categoria # <--- AGORA VAI FUNCIONAR!
        )
    with col3:
        usuario_nome = st.selectbox("Usuário (Quem Registrou):", usuarios_nomes, key="sel_usuario")
    
    # ----------------------------------------
    # LINHA 2: CATEGORIA (Filtro pelo Tipo)
    # ----------------------------------------
    
    id_tipo_selecionado = tipos_map.get(tipo_nome)
    df_cats_filtradas = df_categorias[df_categorias['ID_TipoTransacao'] == id_tipo_selecionado].copy()
    
    if df_cats_filtradas.empty:
         st.warning(f"Não há Categorias cadastradas para o Tipo '{tipo_nome}'. Cadastre uma Categoria.")
         categorias_nomes = ["(Cadastre uma Categoria)"]
    else:
         categorias_nomes = df_cats_filtradas['DSC_CategoriaTransacao'].tolist()

    col4, col5 = st.columns(2)
    with col4:
        # st.selectbox: Categoria - O 'index=0' garante que ele pegará o primeiro item após o reset.
        categoria_nome = st.selectbox(
            "Categoria:", 
            categorias_nomes, 
            key="sel_cat",
            index=0
        )
    
    # ----------------------------------------
    # LINHA 2 CONTINUA: SUBCATEGORIA (Filtro pela Categoria)
    # ----------------------------------------
    
    if categoria_nome == "(Cadastre uma Categoria)":
         df_subs_filtradas = pd.DataFrame() 
         subcategorias_nomes = ["(Cadastre uma Subcategoria)"]
    else:
        # Verifica se a categoria selecionada existe no DataFrame filtrado, evitando erros.
        if categoria_nome in df_cats_filtradas['DSC_CategoriaTransacao'].values:
            id_categoria_selecionada = df_cats_filtradas[df_cats_filtradas['DSC_CategoriaTransacao'] == categoria_nome]['ID_Categoria'].iloc[0]
            
            df_subs_filtradas = df_subcategorias[df_subcategorias['ID_Categoria'] == id_categoria_selecionada].copy()
            
            if df_subs_filtradas.empty:
                 st.warning(f"Não há Subcategorias cadastradas para a Categoria '{categoria_nome}'. Cadastre uma Subcategoria.")
                 subcategorias_nomes = ["(Cadastre uma Subcategoria)"]
            else:
                 subcategorias_nomes = df_subs_filtradas['DSC_SubcategoriaTransacao'].tolist()
        else:
            # Caso a categoria selecionada seja inválida após a troca de Tipo, usa placeholder.
            df_subs_filtradas = pd.DataFrame()
            subcategorias_nomes = ["(Selecione uma Categoria válida)"]


    with col5:
        subcategoria_nome = st.selectbox("Subcategoria:", subcategorias_nomes, key="sel_sub", index=0)
    
    # ----------------------------------------
    # LINHA 3 & 4: VALOR, DESCRIÇÃO, CONTROLE
    # ----------------------------------------
    
    valor_transacao = st.number_input("Valor da Transação:", min_value=0.01, format="%.2f")
    descricao = st.text_area("Descrição Detalhada:", max_chars=100)
    
    st.subheader("Controle de Pagamento")
    col6, col7, col8 = st.columns(3)
    
    with col6:
        quem_pagou = st.selectbox("Quem Pagou:", usuarios_nomes, key="sel_quem_pagou")
    with col7:
        # Ajuste de Label e Opções para "Será Dividida?"
        e_dividido = st.radio(
            "Essa transação será dividida?", 
            ('Não', 'Sim'), 
            horizontal=True, 
            index=0 # Padrão: Não
        )
    with col8:
        # NOVO NOME e Ajuste de Opções para "Saldada/Acertada?"
        foi_dividido = st.radio( # O nome da variável Python continua 'foi_dividido' por simplicidade
            "A transação foi acertada/saldada?", 
            ('Não', 'Sim'), 
            horizontal=True, 
            index=0 # Padrão: Não
        )

    # ----------------------------------------
    # SUBMIT (st.button em vez de st.form_submit_button)
    # ----------------------------------------
    
    # AGORA USAMOS st.button
    submitted = st.button("Registrar Transação")
    
    if submitted:
        # Lógica de validação e inserção (mantida)
        is_valid_category = categoria_nome != "(Cadastre uma Categoria)"
        is_valid_subcategory = subcategoria_nome != "(Cadastre uma Subcategoria)"
        
        # Mapeamento das opções de rádio de volta para N/S para o banco de dados
        # O banco de dados (stg_Transacoes) espera 'S' ou 'N'
        cd_e_dividido_bd = 'S' if e_dividido == 'Sim' else 'N'
        cd_foi_dividido_bd = 'S' if foi_dividido == 'Sim' else 'N'

        if valor_transacao > 0 and descricao and quem_pagou and is_valid_category and is_valid_subcategory:
            
            # Mapeamento de IDs
            id_usuario = int(usuarios_map[usuario_nome])
            id_tipo = int(tipos_map[tipo_nome])
            
            # Usamos .iloc[0] para obter o ID
            id_categoria_final = int(df_cats_filtradas[df_cats_filtradas['DSC_CategoriaTransacao'] == categoria_nome]['ID_Categoria'].iloc[0])
            id_subcategoria_final = int(df_subs_filtradas[df_subs_filtradas['DSC_SubcategoriaTransacao'] == subcategoria_nome]['ID_Subcategoria'].iloc[0])
            
            dados = (data_transacao, id_tipo, tipo_nome, id_categoria_final, categoria_nome, 
                     id_subcategoria_final, subcategoria_nome, id_usuario, usuario_nome, 
                     descricao, valor_transacao, quem_pagou, cd_e_dividido_bd, cd_foi_dividido_bd)
            
            campos = ("DT_DataTransacao", "ID_TipoTransacao", "DSC_TipoTransacao", "ID_Categoria", "DSC_CategoriaTransacao", 
                      "ID_Subcategoria", "DSC_SubcategoriaTransacao", "ID_Usuario", "DSC_NomeUsuario",
                      "DSC_Transacao", "VL_Transacao", "CD_QuemPagou", "CD_EDividido", "CD_FoiDividido") # MANTÉM NOME DA COLUNA SQL
            
            inserir_dados(tabela="stg_Transacoes", dados=dados, campos=campos)
        else:
            st.warning("Verifique se o Valor, Descrição e Categorias/Subcategorias válidas foram selecionadas.")

    st.subheader("Transações em Staging")
    df_stg = consultar_dados("stg_Transacoes", usar_view=False)
    st.dataframe(df_stg, use_container_width=True)

def exibir_detalhe_rateio():
    st.header("Análise de Acerto de Contas")
    
    # ----------------------------------------------------------------------
    # 1. TABELA RESUMO TOTAL: Quem Deve e o Valor (vw_AcertoTotal) <--- NOVO
    # ----------------------------------------------------------------------
    st.subheader("Saldo Total Pendente")

    df_total = consultar_dados("vw_AcertoTotal")

    if df_total.empty:
        st.info("Nenhuma transação para rateio pendente.")
        return # Se não houver dados, para a execução aqui

    # Renomeação do Resumo Total
    df_total.rename(columns={
        'NomeUsuario': 'Usuário',
        'VL_SaldoTotal': 'Saldo Total'
    }, inplace=True)
    
    # Função de estilo (reutilizando a lógica de cor)
    def color_saldo(val):
        color = 'red' if val < 0 else 'green' if val > 0 else 'black'
        return f'color: {color}'

    # Exibição do Resumo Total (Usando .style.format para formatar sem o R$)
    st.dataframe(
        df_total.style.applymap(
            color_saldo, 
            subset=['Saldo Total'] 
        ).format({
            # Formatação para xx.xxx,xx
            'Saldo Total': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        }),
        use_container_width=True
    )

    st.markdown("---")

    # ----------------------------------------------------------------------
    # 2. TABELA CONSOLIDADO MENSAL (vw_AcertoMensal)
    # ----------------------------------------------------------------------
    st.subheader("Saldo Consolidado Mensal")
    df_resumo = consultar_dados("vw_AcertoMensal")

    if df_resumo.empty:
        st.info("Nenhuma transação para rateio pendente. Cadastre uma transação dividida ou marque as transações antigas como saldadas.")
        return

    # Renomeação do Resumo
    df_resumo.rename(columns={
        'CD_QuemDeve': 'Usuário',
        'VL_SaldoAcertoMensal': 'Saldo Líquido'
    }, inplace=True)
    
    # Função de estilo para o Resumo (CORRIGIDO A SINTAXE E ESPERA O VALOR NUMÉRICO)
    def color_saldo_resumo(val):
        # Garante que val é um número
        if isinstance(val, str):
            try:
                val = float(val.replace('.', '').replace(',', '.'))
            except ValueError:
                val = 0
                
        color = 'red' if val < 0 else 'green' if val > 0 else 'black'
        return f'color: {color}'

    # Exibição do Resumo (Usando .style.format para formatar sem o R$)
    st.dataframe(
        df_resumo.style.applymap(
            color_saldo_resumo, 
            subset=['Saldo Líquido'] # Aplica a cor na coluna numérica
        ).format({
            # Formatação para xx.xxx,xx
            'Saldo Líquido': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        }),
        column_order=['Ano', 'Mes', 'Usuário', 'Saldo Líquido'],
        use_container_width=True
    )

    st.markdown("---") 

    # ----------------------------------------------------------------------
    # 3. TABELA DETALHE: Detalhe por Transação (vw_AcertoTransacao)
    # ----------------------------------------------------------------------
    st.subheader("Detalhe das Transações Pendentes de Acerto")
    
    # Usando o nome da View que você indicou: vw_AcertoDetalhe
    df_detalhe = consultar_dados("vw_AcertoDetalhe") 

    # Renomeação do Detalhe
    df_detalhe.rename(columns={
        'DT_DataTransacao': 'Data',
        'DSC_Transacao': 'Descrição',
        'VL_TotalTransacao': 'Total da Transação',
        'CD_QuemPagou': 'Pagador',
        'CD_QuemDeve': 'Usuário',
        'VL_Proporcional': 'Devido (Parte Dele)',
        'VL_AcertoTransacao': 'Acerto Líquido'
    }, inplace=True)
    
    # Função de estilo para o Detalhe
    def color_acerto_detalhe(val):
        # A função de cor agora recebe o valor numérico
        if isinstance(val, str):
            try:
                val = float(val.replace('.', '').replace(',', '.'))
            except ValueError:
                val = 0
                
        color = 'red' if val < 0 else 'green' if val > 0 else 'black'
        return f'color: {color}'

    # Exibição do Detalhe (Usando .style.format para aplicar formatação e cor)
    st.dataframe(
        df_detalhe.style.applymap(
            color_acerto_detalhe, 
            subset=['Acerto Líquido']
        ).format({
            # Formatação de moeda para todas as colunas de valor
            'Total da Transação': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            'Devido (Parte Dele)': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            'Acerto Líquido': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        }),
        use_container_width=True
    )
# --- INTERFACE PRINCIPAL COM MENU (ATUALIZADA) ---

if 'menu_selecionado' not in st.session_state:
    st.session_state.menu_selecionado = "Registrar Transação"

def main():
    st.title("Finanças Pessoais")
    st.sidebar.title("Menu")
    
    # ----------------------------------------------------
    # 1. BOTÃO DE REGISTRO DE FATO (NO TOPO)
    # ----------------------------------------------------
    
    st.sidebar.subheader("Transações")
    
    if st.sidebar.button("📝 Registrar Transação", key="btn_fact_transacao"):
        st.session_state.menu_selecionado = "Transação"
    if st.sidebar.button("💰 Registrar Salário", key="btn_fact_salario"):
        st.session_state.menu_selecionado = "Salário"

    st.sidebar.markdown("---") # Linha separadora para visualização

    # ----------------------------------------------------
    # 2. BOTÕES DE ANÁLISE (NOVO BLOCO)
    # ----------------------------------------------------
    st.sidebar.subheader("Análises e Saldos")
    
    # NOVO BOTÃO DE ACERTO
    if st.sidebar.button("📊 Acerto de Contas", key="btn_analise_acerto"):
        st.session_state.menu_selecionado = "Acerto de Contas"
        
    st.sidebar.markdown("---")

    # ----------------------------------------------------
    # 3. BOTÕES DE CADASTRO DIMENSIONAL
    # ----------------------------------------------------
    
    # Cria o agrupador que se expande e recolhe
    with st.sidebar.expander("Formulários", expanded=True):
        
        # Lista de todas as opções de formulário
        opcoes_cadastro = {
            "Tipos de Transação": formulario_tipo_transacao,
            "Categorias": formulario_categoria,
            "Subcategorias": formulario_subcategoria,
            "Usuários": formulario_usuario
        }
        
        # Cria um botão para cada opção de cadastro
        for nome_opcao, _ in opcoes_cadastro.items():
            # O st.button precisa de uma chave (key) se estiver em um loop
            if st.button(nome_opcao, key=f"btn_{nome_opcao}"):
                # Se o botão for clicado, atualiza o estado da sessão
                st.session_state.menu_selecionado = nome_opcao

    # ----------------------------------------------------
    # 4. EXIBIÇÃO DO FORMULÁRIO SELECIONADO
    # ----------------------------------------------------
    
    # Exibe o formulário com base na opção armazenada no estado da sessão
    opcao_atual = st.session_state.menu_selecionado
    
    if opcao_atual == "Acerto de Contas":
        exibir_detalhe_rateio()
    elif opcao_atual == "Transação":
        formulario_transacao()
    elif opcao_atual == "Salário":
        formulario_salario()
    elif opcao_atual == "Tipos de Transação":
        formulario_tipo_transacao()
    elif opcao_atual == "Categorias":
        formulario_categoria()
    elif opcao_atual == "Subcategorias":
        formulario_subcategoria()
    elif opcao_atual == "Usuários":
        formulario_usuario()

if __name__ == '__main__':
    main()