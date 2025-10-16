import streamlit as st
import pyodbc
import pandas as pd

# --- CONFIGURAÇÃO DA CONEXÃO (MANTENHA SEU AJUSTE AQUI!) ---
# Lembre-se: Se você instalou o SQLEXPRESS, ajuste o SERVER para o nome correto,
# ex: f'DRIVER={DRIVER};SERVER=NOME_DO_SEU_COMPUTADOR\SQLEXPRESS;...'
SERVER = '(localdb)\\MSSQLLocalDB' 
DATABASE = 'financeiro'
DRIVER = '{ODBC Driver 17 for SQL Server}' 
CONNECTION_STRING = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

# --- FUNÇÃO DE CONSULTA (ATUALIZADA) ---
@st.cache_data
def consultar_dados(tabela, usar_view=True):
    """Consulta e retorna todos os dados de uma View (vw_{tabela}) ou Tabela."""
    
    nome_da_fonte = tabela
    # Se usar_view=True, consulta a View com o prefixo 'vw_'
    if usar_view:
        nome_da_fonte = f"vw_{tabela}" 
    
    conn = None
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        
        sql_query = f"SELECT * FROM {nome_da_fonte}"
        
        df = pd.read_sql(sql_query, conn)
        return df
        
    except pyodbc.Error as ex:
        st.error(f"Erro ao consultar {nome_da_fonte}. Verifique se a tabela/view existe e se as colunas estão corretas: {ex}")
        return pd.DataFrame() 
    finally:
        if conn:
            conn.close()


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


# --- INTERFACE PRINCIPAL COM MENU (ATUALIZADA) ---

if 'menu_selecionado' not in st.session_state:
    st.session_state.menu_selecionado = "Tipos de Transação" # Opção padrão

def main():
    st.title("Finanças Pessoais")
    st.sidebar.title("Menu")
    
    # ----------------------------------------------------
    # 1. BOTÕES DE NAVEGAÇÃO NA BARRA LATERAL
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
    # 2. EXIBIÇÃO DO FORMULÁRIO SELECIONADO
    # ----------------------------------------------------
    
    # Exibe o formulário com base na opção armazenada no estado da sessão
    opcao_atual = st.session_state.menu_selecionado
    
    if opcao_atual == "Tipos de Transação":
        formulario_tipo_transacao()
    elif opcao_atual == "Categorias":
        formulario_categoria()
    elif opcao_atual == "Subcategorias":
        formulario_subcategoria()
    elif opcao_atual == "Usuários":
        formulario_usuario()


if __name__ == '__main__':
    main()