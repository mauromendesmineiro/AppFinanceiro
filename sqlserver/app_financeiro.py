import streamlit as st
import pyodbc
import pandas as pd # <-- NOVO: Importa o Pandas para exibir tabelas

# --- CONFIGURAÇÃO DA CONEXÃO (Mantenha seu ajuste aqui!) ---
SERVER = '(localdb)\\MSSQLLocalDB' # Ou 'NOME_DO_SEU_SERVIDOR\\SQLEXPRESS'
DATABASE = 'financeiro'
DRIVER = '{ODBC Driver 17 for SQL Server}' 
CONNECTION_STRING = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

# --- FUNÇÃO DE CONSULTA (NOVO) ---
@st.cache_data # Cache para não consultar o banco toda hora
def consultar_dados(tabela):
    """Consulta e retorna todos os dados de uma tabela."""
    conn = None
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        # Consulta SQL para selecionar todos os registros
        sql_query = f"SELECT * FROM {tabela}"
        
        # Lê os dados do banco e os carrega diretamente em um DataFrame do Pandas
        df = pd.read_sql(sql_query, conn)
        return df
        
    except pyodbc.Error as ex:
        st.error(f"Erro ao consultar dados da {tabela}: {ex}")
        return pd.DataFrame() # Retorna DataFrame vazio em caso de erro
    finally:
        if conn:
            conn.close()


# --- FUNÇÃO DE INSERÇÃO (Mantida do exemplo anterior) ---
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
        
        # Força o Streamlit a limpar o cache e consultar novamente
        st.cache_data.clear() 
        
    except pyodbc.Error as ex:
        st.error(f"Erro ao inserir dados na {tabela}: {ex}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()


# --- FORMULÁRIOS ATUALIZADOS ---

def formulario_tipo_transacao():
    st.header("Cadastro de Tipo de Transação")
    
    with st.form("tipo_form"):
        descricao = st.text_input("Descrição do Tipo (ex: Receita, Despesa)")
        submitted = st.form_submit_button("Inserir Tipo")
        
        if submitted:
            if descricao:
                inserir_dados(
                    tabela="Dim_TipoTransacao",
                    dados=(descricao,),
                    campos=("DSC_TipoTransacao",)
                )
            else:
                st.warning("O campo Descrição é obrigatório.")

    # ---------------------------------------------
    # NOVO: Exibe a tabela de registros existentes
    # ---------------------------------------------
    st.subheader("Registros Existentes (Dim_TipoTransacao)")
    df_tipos = consultar_dados("Dim_TipoTransacao")
    st.dataframe(df_tipos, use_container_width=True)


def formulario_categoria():
    st.header("Cadastro de Categoria")
    
    with st.form("categoria_form"):
        descricao = st.text_input("Descrição da Categoria (ex: Alimentação)")
        submitted = st.form_submit_button("Inserir Categoria")
        
        if submitted and descricao:
            inserir_dados(
                tabela="Dim_Categoria",
                dados=(descricao,),
                campos=("DSC_CategoriaTransacao",)
            )

    # ---------------------------------------------
    # NOVO: Exibe a tabela de registros existentes
    # ---------------------------------------------
    st.subheader("Registros Existentes (Dim_Categoria)")
    df_categorias = consultar_dados("Dim_Categoria")
    st.dataframe(df_categorias, use_container_width=True)

def formulario_subcategoria():
    st.header("Cadastro de Subcategoria")

    # 1. Consulta as Categorias existentes para o campo de seleção
    df_categorias = consultar_dados("Dim_Categoria")
    
    if df_categorias.empty:
        st.warning("Primeiro, cadastre pelo menos uma Categoria (ex: 'Alimentação') na aba ao lado.")
        return

    # Mapeia o nome da categoria para o ID para facilitar a inserção no banco
    categorias_dict = dict(zip(df_categorias['DSC_CategoriaTransacao'], df_categorias['ID_Categoria']))
    categorias_nomes = list(categorias_dict.keys())
    
    with st.form("subcategoria_form"):
        
        # 2. Campo de seleção (Dropdown) da Categoria
        categoria_selecionada_nome = st.selectbox(
            "Selecione a Categoria Pai:",
            categorias_nomes
        )
        
        # 3. Campo de entrada da Subcategoria
        descricao_subcategoria = st.text_input("Descrição da Subcategoria (ex: Almoço, Supermercado)")
        
        submitted = st.form_submit_button("Inserir Subcategoria")
        
        if submitted:
            if descricao_subcategoria and categoria_selecionada_nome:
                
                # Obtém o ID da categoria selecionada pelo nome
                id_categoria = categorias_dict[categoria_selecionada_nome]
                
                # Insere o ID_Categoria e a descrição da subcategoria
                inserir_dados(
                    tabela="Dim_Subcategoria",
                    dados=(id_categoria, descricao_subcategoria),
                    campos=("ID_Categoria", "DSC_SubcategoriaTransacao")
                )
            else:
                st.warning("Todos os campos são obrigatórios.")

    # ---------------------------------------------
    # NOVO: Exibe a tabela de registros existentes
    # ---------------------------------------------
    st.subheader("Registros Existentes (Dim_Subcategoria)")
    # Se você quiser um relatório mais amigável que mostre o nome da categoria:
    # Seria necessário um SELECT com JOIN. Por simplicidade, exibimos os IDs.
    df_subcategorias = consultar_dados("Dim_Subcategoria")
    st.dataframe(df_subcategorias, use_container_width=True)

# --- INTERFACE PRINCIPAL ---

def main():
    st.title("Sistema de Cadastro Financeiro Local")
    st.sidebar.title("Menu de Cadastros")
    
    # Cria o menu de seleção na barra lateral
    opcao = st.sidebar.radio(
        "Selecione o Cadastro:",
        ("Tipo de Transação", "Categoria", "Subcategoria") # Removi o "(Em Breve)"
    )
    
    # Exibe o formulário correspondente à opção
    if opcao == "Tipo de Transação":
        formulario_tipo_transacao()
    elif opcao == "Categoria":
        formulario_categoria()
    elif opcao == "Subcategoria":
        formulario_subcategoria() # <-- NOVO: Chama o novo formulário

if __name__ == '__main__':
    main()