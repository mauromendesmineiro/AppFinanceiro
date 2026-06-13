import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
from dateutil.relativedelta import relativedelta
import psycopg2 
from psycopg2 import sql
import plotly.colors as colors
import numpy as np
import plotly.graph_objects as go

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    layout="wide",  # Define a largura máxima como a largura do navegador
    initial_sidebar_state="auto"
)

#@st.cache_resource(ttl=600) 
def get_connection():
    # As credenciais são carregadas do secrets.toml (bloco [postgresql])
    conn_details = st.secrets["postgresql"] 
    
    conn = psycopg2.connect(
        host=conn_details["server"],
        database=conn_details["database"],
        user=conn_details["username"],
        password=conn_details["password"],
        port=conn_details["port"],
        sslmode='require' # O Neon exige SSL
    )
    return conn

@st.cache_data(ttl=3600)
def consultar_dados(tabela_ou_view, usar_view=True): 
    """
    Consulta dados de uma tabela ou view e retorna um DataFrame.
    
    Parâmetros:
        tabela_ou_view (str): Nome da tabela ou view a ser consultada.
        usar_view (bool): Parâmetro adicionado para compatibilidade com 
                          a chamada de outras funções (não tem efeito 
                          no corpo desta função atualmente).
    """
    tabela_ou_view = tabela_ou_view.lower() 

    conn = None 
    df = pd.DataFrame()
    
    try:
        # 1. Tenta obter a conexão (a falha aqui é a causa raiz do problema de ambiente)
        conn = get_connection() 
        
        # 2. Verificação explícita para o caso de get_connection() falhar e retornar None
        if conn is None:
            raise Exception("A conexão ao banco de dados falhou ou retornou None.") 
            
        # 3. Monta a query com segurança
        sql_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(tabela_ou_view))
        
        # 4. Executa a query
        df = pd.read_sql(sql_query.as_string(conn), conn)
        
    # 5. Captura TypeErrors (o erro que estava ocorrendo), erros de banco e exceções gerais
    except (psycopg2.Error, TypeError, Exception) as e:
        # Exibe um erro amigável
        st.error(f"Erro ao conectar ou consultar o banco de dados para a tabela '{tabela_ou_view}'. Detalhes: {e}")
        df = pd.DataFrame() 
        
    finally:
        # 6. Garante que a conexão seja fechada
        if conn is not None:
            conn.close()
            
    return df

def limpar_cache_dados():
    """Limpa o cache do Streamlit para forçar a recarga dos dados do banco."""
    # st.cache_data é a decorator que usamos na consultar_dados
    st.cache_data.clear() 
    st.success("Cache de dados limpo. Recarregando as análises...")
    st.rerun()

def inserir_dados(tabela, dados, campos):
    conn = None
    tabela_lower = tabela.lower()
    
    # GARANTE que os nomes de campo também estão em minúsculo (Padrão PostgreSQL)
    campos_lower = [c.lower() for c in campos]
    
    # Constrói o SQL: Exemplo: INSERT INTO dim_tipotransacao (dsc_tipotransacao) VALUES (%s)
    placeholders = ', '.join(['%s'] * len(dados))
    sql = f"INSERT INTO {tabela_lower} ({', '.join(campos_lower)}) VALUES ({placeholders})"
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # 1. EXECUÇÃO: Passa o SQL e os dados (a tupla de valores)
        # Exemplo: cursor.execute("...", ('Receita',))
        cursor.execute(sql, dados) 
        
        # 2. COMMIT: ESSENCIAL para salvar os dados
        conn.commit() 
        
        # Feedback de Sucesso no Streamlit (Opcional, mas recomendado)
        st.success(f"Registro inserido com sucesso na tabela {tabela_lower.upper()}!")
        
        return True
        
    except psycopg2.Error as ex: # <<< LINHA 82 (Provavelmente este bloco)
        # ESTE BLOCO DE CÓDIGO TEM QUE SER RECÉUADO
        st.error(f"Erro do banco de dados ao inserir: {ex}")
        if conn: conn.rollback()
        
    except Exception as e:
        # ESTE BLOCO DE CÓDIGO TAMBÉM TEM QUE SER RECÉUADO
        st.error(f"Erro inesperado: {e}")
        if conn: conn.rollback()
        
    finally: # <<< LINHA 85 (Deve estar alinhada com try e except)
        if conn: conn.close()

def formulario_tipo_transacao():
    st.header("Cadastro e Manutenção de Tipo de Transação")
    
    # ----------------------------------------------------
    # A) CADASTRO (MANTIDO)
    # ----------------------------------------------------
    st.subheader("1. Inserir Novo Tipo")
    
    with st.form("tipo_form"):
        # Campo de entrada
        descricao = st.text_input("Descrição do Tipo (ex: Receita, Despesa)")
        
        submitted = st.form_submit_button("Inserir Novo Tipo")
        
        if submitted:
            if descricao:
                # Assume que a função inserir_dados está definida
                inserir_dados(
                    tabela="dim_tipotransacao",
                    dados=(descricao,),
                    campos=("dsc_tipotransacao",)
                )
            else:
                st.warning("O campo Descrição é obrigatório.")

    # ----------------------------------------------------
    # B) VISUALIZAÇÃO E SELEÇÃO MANUAL (CORREÇÃO DA LÓGICA)
    # ----------------------------------------------------
    st.markdown("---")
    st.subheader("2. Editar ou Excluir Registros Existentes")
    
    df_tipos = consultar_dados("dim_tipotransacao")
    
    if df_tipos.empty:
        st.info("Nenhum tipo de transação registrado.")
        return

    # Renomeação para exibição e seleção
    df_exibicao = df_tipos.rename(columns={'id_tipotransacao': 'ID', 'dsc_tipotransacao': 'Descrição'})[['ID', 'Descrição']]
    
    # Exibe a tabela completa para referência
    st.dataframe(df_exibicao, hide_index=True, use_container_width=True)
    
    # --- SELEÇÃO MANUAL DO ID ---
    lista_ids = [''] + df_exibicao['ID'].astype(str).tolist()
    id_selecionado_str = st.selectbox(
        "Selecione o ID do Tipo para Editar/Excluir:", 
        options=lista_ids, 
        key="tipo_id_selector" 
    )
    
    id_selecionado = None
    descricao_atual = None
    
    if id_selecionado_str and id_selecionado_str != '':
        try:
            id_selecionado = int(id_selecionado_str)
            
            # Busca os dados do ID selecionado no DataFrame de exibição
            dados_selecionados = df_exibicao[df_exibicao['ID'] == id_selecionado].iloc[0]
            descricao_atual = dados_selecionados['Descrição']
            
            st.markdown(f"#### Manutenção do Tipo: ID {id_selecionado} - {descricao_atual}")
            
            # --- BLOC DA EDIÇÃO ---
            with st.form("edicao_tipo_form"):
                novo_descricao = st.text_input("Nova Descrição:", value=descricao_atual)
                
                # Botões de Ação
                col_edit, col_delete = st.columns([1, 1])
                
                with col_edit:
                    edit_submitted = st.form_submit_button("Salvar Edição", type="secondary")
                with col_delete:
                     delete_clicked = st.form_submit_button("🔴 Excluir Registro", type="primary")

                if edit_submitted:
                    if novo_descricao and novo_descricao != descricao_atual:
                        # Assumindo que atualizar_registro_dimensao está definida
                        campos_valores = {"dsc_tipotransacao": novo_descricao}
                        if atualizar_registro_dimensao("dim_tipotransacao", "id_tipotransacao", id_selecionado, campos_valores):
                            st.success(f"Tipo ID {id_selecionado} atualizado para '{novo_descricao}'.")
                            st.rerun()
                    elif novo_descricao == descricao_atual:
                        st.info("Nenhuma alteração detectada para salvar.")
                    else:
                        st.warning("A descrição não pode ser vazia.")
                
                if delete_clicked:
                    # Armazena o ID no estado da sessão para a confirmação fora do formulário
                    # Usando chaves específicas para esta dimensão (tipo)
                    st.session_state.confirm_delete_id_tipo = id_selecionado
                    st.session_state.confirm_delete_nome_tipo = descricao_atual
                    st.rerun() 
            # --- FIM BLOC DA EDIÇÃO ---

        except Exception as e:
            st.error(f"Erro ao carregar dados do ID: {e}")
            
    
    # --- LÓGICA DE CONFIRMAÇÃO DE EXCLUSÃO ---
    if st.session_state.get('confirm_delete_id_tipo'):
        id_del = st.session_state.confirm_delete_id_tipo
        nome_del = st.session_state.confirm_delete_nome_tipo
        
        st.markdown("---")
        st.error(f"⚠️ CONFIRMAÇÃO DE EXCLUSÃO: Tem certeza que deseja EXCLUIR o tipo '{nome_del}' (ID {id_del})? Esta ação é irreversível e pode causar erros de integridade em Transações.")
        
        col_conf_sim, col_conf_nao = st.columns(2)
        
        with col_conf_sim:
            if st.button("SIM, EXCLUIR PERMANENTEMENTE", key="final_delete_tipo_sim"):
                # Assumindo que deletar_registro_dimensao está definida
                if deletar_registro_dimensao("dim_tipotransacao", "id_tipotransacao", id_del):
                    st.success(f"Tipo ID {id_del} excluído com sucesso.")
                    st.session_state.confirm_delete_id_tipo = None
                    st.rerun()
        with col_conf_nao:
            if st.button("CANCELAR Exclusão", key="final_delete_tipo_nao"):
                st.session_state.confirm_delete_id_tipo = None
                st.rerun()

def formulario_categoria():
    st.header("Cadastro e Manutenção de Categorias")

    # 1. Busca os dados de Tipo de Transação para os dropdowns (dim_tipotransacao)
    # NOTE: O nome da tabela é em minúsculo
    df_tipos = consultar_dados("dim_tipotransacao")
    
    if df_tipos.empty:
        st.warning("É necessário cadastrar pelo menos um Tipo de Transação (Receita/Despesa) antes de cadastrar Categorias.")
        return
        
    # Mapeamento do Tipo (Nome -> ID). Colunas do DataFrame são lidas em minúsculo.
    tipos_dict = dict(zip(df_tipos['dsc_tipotransacao'], df_tipos['id_tipotransacao']))
    tipos_nomes = list(tipos_dict.keys())

    # ----------------------------------------------------
    # A) CADASTRO 
    # ----------------------------------------------------
    st.subheader("1. Inserir Nova Categoria")
    
    with st.form("categoria_form"):
        # Campo de entrada
        tipo_selecionado = st.selectbox(
            "Selecione o Tipo de Transação Pai:",
            tipos_nomes
        )
        descricao = st.text_input("Descrição da Categoria (ex: Alimentação, Residência)")
        
        submitted = st.form_submit_button("Inserir Nova Categoria")
        
        if submitted:
            if descricao:
                id_tipo = tipos_dict[tipo_selecionado]
                
                # Inserção na tabela dim_categoria
                # Nomes de tabelas e campos em minúsculo
                inserir_dados(
                    tabela="dim_categoria",
                    dados=(id_tipo, descricao,),
                    campos=("id_tipotransacao", "dsc_categoriatransacao") 
                )
            else:
                st.warning("A Descrição e o Tipo de Transação são obrigatórios.")

    # ----------------------------------------------------
    # B) VISUALIZAÇÃO E SELEÇÃO MANUAL (EDIÇÃO/EXCLUSÃO)
    # ----------------------------------------------------
    st.markdown("---")
    st.subheader("2. Editar ou Excluir Registros Existentes")
    
    # USANDO A VIEW INFORMADA PELO USUÁRIO (vw_dim_categoria)
    df_categorias = consultar_dados("vw_dim_categoria") 
    
    if df_categorias.empty:
        st.info("Nenhuma categoria registrada.")
        return

    # ----------------------------------------------------------------------
    # CORREÇÃO CRÍTICA DO KEYERROR: As chaves do dicionário são em minúsculo
    # ----------------------------------------------------------------------
    df_exibicao = df_categorias.rename(columns={
        'id': 'ID',                         # Coluna do DF: 'id' -> Exibição: 'ID'
        'categoria': 'Descrição',           # Coluna do DF: 'categoria' -> Exibição: 'Descrição'
        'tipodetransacao': 'Tipo Pai',      # Coluna do DF: 'tipodetransacao' -> Exibição: 'Tipo Pai'
        'datacriacao': 'DataCriacao'        # Inclui a coluna de data (minúscula -> maiúscula)
    })[['ID', 'Descrição', 'Tipo Pai']] 

    # Exibe a tabela completa para referência
    st.dataframe(df_exibicao, hide_index=True, use_container_width=True)
    
    # --- SELEÇÃO MANUAL DO ID ---
    lista_ids = [''] + df_exibicao['ID'].astype(str).tolist()
    id_selecionado_str = st.selectbox(
        "Selecione o ID da Categoria para Editar/Excluir:", 
        options=lista_ids, 
        key="categoria_id_selector" 
    )
    
    id_selecionado = None
    
    if id_selecionado_str and id_selecionado_str != '':
        try:
            id_selecionado = int(id_selecionado_str)
            
            # Busca os dados do ID selecionado
            dados_selecionados = df_exibicao[df_exibicao['ID'] == id_selecionado].iloc[0]
            descricao_atual = dados_selecionados['Descrição']
            tipo_pai_atual = dados_selecionados['Tipo Pai']
            
            st.markdown(f"#### Manutenção da Categoria: ID {id_selecionado} - {descricao_atual}")
            
            # --- BLOC DA EDIÇÃO ---
            with st.form("edicao_categoria_form"):
                
                # Permite mudar o Tipo Pai
                novo_tipo_pai = st.selectbox(
                    "Mudar Tipo de Transação Pai:", 
                    tipos_nomes,
                    index=tipos_nomes.index(tipo_pai_atual)
                )
                novo_descricao = st.text_input("Nova Descrição:", value=descricao_atual)
                
                # Botões de Ação
                col_edit, col_delete = st.columns([1, 1])
                
                with col_edit:
                    edit_submitted = st.form_submit_button("Salvar Edição", type="secondary")
                with col_delete:
                    delete_clicked = st.form_submit_button("🔴 Excluir Registro", type="primary")

                if edit_submitted:
                    id_novo_tipo = tipos_dict[novo_tipo_pai]
                    id_tipo_pai_atual = tipos_dict[tipo_pai_atual]
                    
                    # Verifica se houve alteração
                    if novo_descricao != descricao_atual or id_novo_tipo != id_tipo_pai_atual:
                        
                        # Campos para o UPDATE
                        campos_valores = {
                            "id_tipotransacao": id_novo_tipo, 
                            "dsc_categoriatransacao": novo_descricao
                        }
                        
                        if atualizar_registro_dimensao("dim_categoria", "id_categoria", id_selecionado, campos_valores):
                            st.success(f"Categoria ID {id_selecionado} atualizada com sucesso.")
                            st.rerun()
                    else:
                        st.info("Nenhuma alteração detectada para salvar.")
                
                if delete_clicked:
                    st.session_state.confirm_delete_id_cat = id_selecionado
                    st.session_state.confirm_delete_nome_cat = descricao_atual
                    st.rerun() 
            # --- FIM BLOC DA EDIÇÃO ---

        except Exception as e:
            st.error(f"Erro ao carregar dados do ID. Detalhe: {e}")
            
    
    # --- LÓGICA DE CONFIRMAÇÃO DE EXCLUSÃO ---
    if st.session_state.get('confirm_delete_id_cat'):
        id_del = st.session_state.confirm_delete_id_cat
        nome_del = st.session_state.confirm_delete_nome_cat
        
        st.markdown("---")
        st.error(f"⚠️ CONFIRMAÇÃO DE EXCLUSÃO: Tem certeza que deseja EXCLUIR a Categoria '{nome_del}' (ID {id_del})? Esta ação é irreversível e impedirá a exclusão se houver Subcategorias ou Transações vinculadas.")
        
        col_conf_sim, col_conf_nao = st.columns(2)
        
        with col_conf_sim:
            if st.button("SIM, EXCLUIR PERMANENTEMENTE", key="final_delete_cat_sim"):
                # O nome da tabela e da coluna ID são passados em minúsculo
                if deletar_registro_dimensao("dim_categoria", "id_categoria", id_del):
                    st.success(f"Categoria ID {id_del} excluída com sucesso.")
                    st.session_state.confirm_delete_id_cat = None
                    st.rerun()
        with col_conf_nao:
            if st.button("CANCELAR Exclusão", key="final_delete_cat_nao"):
                st.session_state.confirm_delete_id_cat = None
                st.rerun()

def formulario_subcategoria():
    st.header("Cadastro e Manutenção de Subcategorias")

    # 1. Busca os dados de Categoria para os dropdowns (dim_categoria)
    df_categorias = consultar_dados("dim_categoria")
    
    if df_categorias.empty:
        st.warning("É necessário cadastrar pelo menos uma Categoria antes de cadastrar Subcategorias.")
        return
        
    # Mapeamento da Categoria (Nome -> ID).
    categorias_dict = dict(zip(df_categorias['dsc_categoriatransacao'], df_categorias['id_categoria']))
    categorias_nomes = list(categorias_dict.keys())

    # ----------------------------------------------------
    # A) CADASTRO 
    # ----------------------------------------------------
    st.subheader("1. Inserir Nova Subcategoria")
    
    with st.form("subcategoria_form"):
        # Campo de entrada
        categoria_selecionada = st.selectbox(
            "Selecione a Categoria Pai:",
            categorias_nomes
        )
        # Coluna de descrição no DB: dsc_subcategoriatransacao
        descricao = st.text_input("Descrição da Subcategoria (ex: Aluguel, Internet)")
        
        submitted = st.form_submit_button("Inserir Nova Subcategoria")
        
        if submitted:
            if descricao:
                id_categoria = categorias_dict[categoria_selecionada]
                
                # Inserção na tabela dim_subcategoria
                inserir_dados(
                    tabela="dim_subcategoria",
                    dados=(id_categoria, descricao,),
                    campos=("id_categoria", "dsc_subcategoriatransacao") 
                )
            else:
                st.warning("A Descrição e a Categoria são obrigatórias.")

    # ----------------------------------------------------
    # B) VISUALIZAÇÃO E SELEÇÃO MANUAL (EDIÇÃO/EXCLUSÃO)
    # ----------------------------------------------------
    st.markdown("---")
    st.subheader("2. Editar ou Excluir Registros Existentes")
    
    # *** USANDO A VIEW INFORMADA PELO USUÁRIO (vw_dim_subcategoria) ***
    df_subcategorias = consultar_dados("vw_dim_subcategoria") 
    
    if df_subcategorias.empty:
        st.info("Nenhuma subcategoria registrada.")
        return

    # *** CORREÇÃO DO KEY ERROR: Mapeando chaves em minúsculo ***
    df_exibicao = df_subcategorias.rename(columns={
        'id': 'ID',                         # Mapeia 'id' (minúsculo)
        'subcategoria': 'Descrição',        # Mapeia 'subcategoria' (minúsculo)
        'categoria': 'Categoria Pai',       # Mapeia 'categoria' (minúsculo)
        'datacriacao': 'DataCriacao'        # Coluna de data
    })[['ID', 'Descrição', 'Categoria Pai']] # Seleção final usa os nomes de exibição
    
    # Exibe a tabela completa para referência
    st.dataframe(df_exibicao, hide_index=True, use_container_width=True)
    
    # --- SELEÇÃO MANUAL DO ID ---
    lista_ids = [''] + df_exibicao['ID'].astype(str).tolist()
    id_selecionado_str = st.selectbox(
        "Selecione o ID da Subcategoria para Editar/Excluir:", 
        options=lista_ids, 
        key="subcategoria_id_selector" 
    )
    
    id_selecionado = None
    
    if id_selecionado_str and id_selecionado_str != '':
        try:
            id_selecionado = int(id_selecionado_str)
            
            # Busca os dados do ID selecionado
            dados_selecionados = df_exibicao[df_exibicao['ID'] == id_selecionado].iloc[0]
            descricao_atual = dados_selecionados['Descrição']
            categoria_pai_atual = dados_selecionados['Categoria Pai']
            
            st.markdown(f"#### Manutenção da Subcategoria: ID {id_selecionado} - {descricao_atual}")
            
            # --- BLOC DA EDIÇÃO ---
            with st.form("edicao_subcategoria_form"):
                
                # Permite mudar a Categoria Pai
                novo_categoria_pai = st.selectbox(
                    "Mudar Categoria Pai:", 
                    categorias_nomes,
                    index=categorias_nomes.index(categoria_pai_atual)
                )
                novo_descricao = st.text_input("Nova Descrição:", value=descricao_atual)
                
                # Botões de Ação
                col_edit, col_delete = st.columns([1, 1])
                
                with col_edit:
                    edit_submitted = st.form_submit_button("Salvar Edição", type="secondary")
                with col_delete:
                    delete_clicked = st.form_submit_button("🔴 Excluir Registro", type="primary")

                if edit_submitted:
                    id_nova_categoria = categorias_dict[novo_categoria_pai]
                    
                    # Busca o ID da Categoria Pai atual 
                    id_categoria_pai_atual = categorias_dict[categoria_pai_atual]
                    
                    # Verifica se houve alteração
                    if novo_descricao != descricao_atual or id_nova_categoria != id_categoria_pai_atual:
                        
                        # Campos para o UPDATE
                        campos_valores = {
                            "id_categoria": id_nova_categoria, # Atualiza o ID da Categoria
                            "dsc_subcategoriatransacao": novo_descricao
                        }
                        
                        if atualizar_registro_dimensao("dim_subcategoria", "id_subcategoria", id_selecionado, campos_valores):
                            st.success(f"Subcategoria ID {id_selecionado} atualizada com sucesso.")
                            st.rerun()
                    else:
                        st.info("Nenhuma alteração detectada para salvar.")
                
                if delete_clicked:
                    st.session_state.confirm_delete_id_sub = id_selecionado
                    st.session_state.confirm_delete_nome_sub = descricao_atual
                    st.rerun() 
            # --- FIM BLOC DA EDIÇÃO ---

        except Exception as e:
            st.error(f"Erro ao carregar dados do ID. Verifique se o ID existe ou se os nomes das colunas da View estão corretos: {e}")
            
    
    # --- LÓGICA DE CONFIRMAÇÃO DE EXCLUSÃO ---
    if st.session_state.get('confirm_delete_id_sub'):
        id_del = st.session_state.confirm_delete_id_sub
        nome_del = st.session_state.confirm_delete_nome_sub
        
        st.markdown("---")
        st.error(f"⚠️ CONFIRMAÇÃO DE EXCLUSÃO: Tem certeza que deseja EXCLUIR a Subcategoria '{nome_del}' (ID {id_del})? Esta ação é irreversível.")
        
        col_conf_sim, col_conf_nao = st.columns(2)
        
        with col_conf_sim:
            if st.button("SIM, EXCLUIR PERMANENTEMENTE", key="final_delete_sub_sim"):
                # O deletar_registro_dimensao já lida com o erro de Foreign Key
                if deletar_registro_dimensao("dim_subcategoria", "id_subcategoria", id_del):
                    st.success(f"Subcategoria ID {id_del} excluída com sucesso.")
                    st.session_state.confirm_delete_id_sub = None
                    st.rerun()
        with col_conf_nao:
            if st.button("CANCELAR Exclusão", key="final_delete_sub_nao"):
                st.session_state.confirm_delete_id_sub = None
                st.rerun()

def formulario_usuario():
    st.header("Cadastro de Usuário")
    
    with st.form("usuario_form"):
        # Campo de entrada
        nome = st.text_input("Nome do Usuário:")
        
        submitted = st.form_submit_button("Inserir Usuário")
        
        if submitted:
            if nome:
                inserir_dados(
                    tabela="dim_usuario",
                    dados=(nome,),
                    campos=("dsc_nome",) # Coluna original
                )
            else:
                st.warning("O campo Nome é obrigatório.")

    # Exibe a tabela usando a View (nomes amigáveis: ID, Nome)
    st.subheader("Registros Existentes")
    df_usuarios = consultar_dados("dim_usuario")
    st.dataframe(df_usuarios, use_container_width=True)

def formulario_salario():
    st.header("Registro de Salário")

    # 1. Consulta o Usuário para o Dropdown
    try:
        # CORREÇÃO: Argumento 'usar_view=False' REMOVIDO
        df_usuarios = consultar_dados("dim_usuario")
    except Exception:
        df_usuarios = pd.DataFrame(columns=['id_usuario', 'dsc_nome'])

    if df_usuarios.empty:
        st.warning("Primeiro, cadastre pelo menos um Usuário na aba 'Usuário'.")
        # Removido o check 'consultar_dados' no globals, pois ele deve ser uma importação garantida.
        return

    # Mapeamento do Usuário (Nome -> ID)
    usuarios_dict = dict(zip(df_usuarios['dsc_nome'], df_usuarios['id_usuario']))
    usuarios_nomes = list(usuarios_dict.keys())
    
    # ------------------ BLOC FORMULÁRIO ------------------
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
                
                # --- FUNÇÃO DE INSERÇÃO ---
                # Esta função deve estar definida no seu main.py
                # Ex: inserir_dados(tabela, dados, campos)
                # -------------------------
                inserir_dados(
                    tabela="fact_salario", 
                    dados=(id_usuario, valor_salario, data_recebimento, observacao),
                    campos=("id_usuario", "vl_salario", "dt_recebimento", "dsc_observacao")
                )
                # --------------------------------------------------------
                
                st.success(f"Salário de R${valor_salario:.2f} registrado para {usuario_selecionado_nome}!")
            else:
                st.warning("O Valor do Salário deve ser maior que zero.")
    # ------------------ FIM FORMULÁRIO ------------------


    # Exibe a tabela com as colunas ajustadas, usando a View
    st.subheader("Salários Registrados")
    
    # 1. Consulta a View que já tem o Nome do Usuário, Ano e Mês
    # CORREÇÃO: Argumento 'usar_view=True' REMOVIDO
    df_salarios = consultar_dados("vw_fact_salarios") 

    if not df_salarios.empty:
        
        # 2. Função de Formatação (deve ser definida no escopo global ou localmente)
        def formatar_moeda(x):
            return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        # 3. Renomeação das Colunas para Exibição
        df_exibicao = df_salarios.rename(columns={
            'nomeusuario': 'Usuário', 
            'vl_salario': 'Valor do Salário',
            'dsc_observacao': 'Descrição do Salário',
        })
        
        # 4. Seleção das Colunas Finais
        colunas_finais = [
            "id_salario",
            "Usuário", 
            "Valor do Salário",
            "dt_recebimento",
            "Descrição do Salário",
            "ano",
            "mes"
        ]

        # 5. Aplica a Formatação de Moeda
        # Streamlit exibe melhor formatação nativa se o tipo for float.
        # Se precisar de formatação específica (R$ X.XXX,XX), use st.dataframe.
        df_exibicao['Valor do Salário'] = df_exibicao['Valor do Salário'].apply(formatar_moeda) 
        
        # 6. Exibe o DataFrame com os nomes de colunas corretos
        st.dataframe(df_exibicao[colunas_finais], hide_index=True, use_container_width=True)

    else:
        st.info("Nenhum salário registrado.")

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
    df_tipos = consultar_dados("dim_tipotransacao")
    df_categorias = consultar_dados("dim_categoria")
    df_subcategorias = consultar_dados("dim_subcategoria")
    df_usuarios = consultar_dados("dim_usuario") 

    # --- DADOS DO USUÁRIO LOGADO (VINCULAÇÃO AUTOMÁTICA) ---
    try:
        id_usuario_logado = st.session_state.id_usuario_logado
        # Usamos o nome completo (dsc_nome) para exibição e registro na tabela
        nome_usuario = st.session_state.nome_completo # <<--- ALTERAÇÃO PRINCIPAL AQUI
        
    except AttributeError:
        st.error("Erro de Sessão: As variáveis de usuário logado (id_usuario_logado e nome_completo) não estão configuradas na sessão.")
        return
    
    # CRÍTICO: ALTERAÇÃO NA EXIBIÇÃO PARA USAR O NOME COMPLETO
    #st.info(f"Usuário (Quem Registrou) **automaticamente** definido como: **{nome_usuario}**")
    # --------------------------------------------------------

    # Validação Mínima
    if df_tipos.empty or df_categorias.empty or df_subcategorias.empty or df_usuarios.empty:
        st.warning("É necessário cadastrar: Usuários, Tipos, Categorias e Subcategorias. Verifique as tabelas de dimensões.")
        return

    # Mapeamentos
    tipos_map = dict(zip(df_tipos['dsc_tipotransacao'], df_tipos['id_tipotransacao']))
    usuarios_nomes = df_usuarios['dsc_nome'].tolist()
    tipos_nomes = list(tipos_map.keys())
    
    # ----------------------------------------
    # LINHA 1: DATA, TIPO 
    # ----------------------------------------
    col1, col2 = st.columns(2) 
    with col1:
        data_transacao = st.date_input("Data da Transação:", datetime.date.today())
    with col2:
        tipo_nome = st.selectbox(
            "Tipo de Transação:", 
            tipos_nomes, 
            key="sel_tipo", 
            # on_change=reset_categoria 
        )
    
    # --- LOGICA DE FILTRAGEM DE CATEGORIAS ---
    id_tipo_selecionado = tipos_map.get(tipo_nome)
    if id_tipo_selecionado is not None:
        df_cats_filtradas = df_categorias[df_categorias['id_tipotransacao'] == id_tipo_selecionado].copy()
    else:
        df_cats_filtradas = pd.DataFrame()
        
    if df_cats_filtradas.empty:
        st.warning(f"Não há Categorias cadastradas para o Tipo '{tipo_nome}'. Cadastre uma Categoria.")
        categorias_nomes = ["(Cadastre uma Categoria)"]
    else:
        categorias_nomes = df_cats_filtradas['dsc_categoriatransacao'].tolist()

    col4, col5 = st.columns(2)
    with col4:
        categoria_nome = st.selectbox(
            "Categoria:", 
            categorias_nomes, 
            key="sel_cat",
            index=0
        )
        
    # --- LOGICA DE FILTRAGEM DE SUBCATEGORIAS ---
    df_subs_filtradas = pd.DataFrame()
    subcategorias_nomes = ["(Selecione uma Categoria válida)"]
    
    if categoria_nome != "(Cadastre uma Categoria)" and categoria_nome in df_cats_filtradas['dsc_categoriatransacao'].values:
        
        id_categoria_selecionada = df_cats_filtradas[df_cats_filtradas['dsc_categoriatransacao'] == categoria_nome]['id_categoria'].iloc[0]
        df_subs_filtradas = df_subcategorias[df_subcategorias['id_categoria'] == id_categoria_selecionada].copy()
        
        if df_subs_filtradas.empty:
            st.warning(f"Não há Subcategorias cadastradas para a Categoria '{categoria_nome}'. Cadastre uma Subcategoria.")
            subcategorias_nomes = ["(Cadastre uma Subcategoria)"]
        else:
            subcategorias_nomes = df_subs_filtradas['dsc_subcategoriatransacao'].tolist()

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
        e_dividido = st.radio(
            "Essa transação será dividida?", 
            ('Não', 'Sim'), 
            horizontal=True, 
            index=0
        )
    with col8:
        foi_dividido = st.radio(
            "A transação foi acertada/saldada?", 
            ('Não', 'Sim'), 
            horizontal=True, 
            index=0
        )

    # ----------------------------------------
    # SUBMIT
    # ----------------------------------------
    
    submitted = st.button("Registrar Transação")
    
    if submitted:
        # Mapeamento
        cd_e_dividido_bd = 'S' if e_dividido == 'Sim' else 'N'
        cd_foi_dividido_bd = 'S' if foi_dividido == 'Sim' else 'N'

        is_valid_category = categoria_nome not in ["(Cadastre uma Categoria)", "(Selecione uma Categoria válida)"]
        is_valid_subcategory = subcategoria_nome not in ["(Cadastre uma Subcategoria)", "(Selecione uma Categoria válida)"]

        if valor_transacao > 0 and descricao and quem_pagou and is_valid_category and is_valid_subcategory:
            
            # --- USO DOS DADOS VINCULADOS ---
            id_usuario_final = id_usuario_logado
            usuario_nome_final = nome_usuario # <--- NOME COMPLETO USADO NO REGISTRO
            # -------------------------------
            
            id_tipo = int(tipos_map[tipo_nome])
            
            id_categoria_final = int(df_cats_filtradas[df_cats_filtradas['dsc_categoriatransacao'] == categoria_nome]['id_categoria'].iloc[0])
            id_subcategoria_final = int(df_subs_filtradas[df_subs_filtradas['dsc_subcategoriatransacao'] == subcategoria_nome]['id_subcategoria'].iloc[0])
            
            dados = (data_transacao, id_tipo, tipo_nome, id_categoria_final, categoria_nome, 
                     id_subcategoria_final, subcategoria_nome, id_usuario_final, usuario_nome_final, 
                     descricao, valor_transacao, quem_pagou, cd_e_dividido_bd, cd_foi_dividido_bd)
            
            campos = ("dt_datatransacao", "id_tipotransacao", "dsc_tipotransacao", "id_categoria", "dsc_categoriatransacao", 
                      "id_subcategoria", "dsc_subcategoriatransacao", "id_usuario", "dsc_nomeusuario",
                      "dsc_transacao", "vl_transacao", "cd_quempagou", "cd_edividido", "cd_foidividido") 
            
            # A função inserir_dados deve estar definida
            inserir_dados(tabela="stg_transacoes", dados=dados, campos=campos)
            st.success(f"Transação '{descricao}' registrada com sucesso por {usuario_nome_final}!")
        else:
            st.warning("Verifique se o Valor, Descrição e Categorias/Subcategorias válidas foram selecionadas.")

    st.subheader("Transações em Staging")
    df_stg = consultar_dados("vw_stg_transacoes") 
    st.dataframe(df_stg, use_container_width=True)

def exibir_detalhe_rateio():
    st.header("Análise de Acerto de Contas")
    
    # -------------------------------------------------------------
    # 1. TABELA RESUMO TOTAL: Quem Deve e o Valor (vw_acertototal)
    # -------------------------------------------------------------
    st.subheader("Saldo Total Pendente")

    df_total = consultar_dados("vw_acertototal")

    if df_total.empty:
        st.info("Nenhuma transação para rateio pendente.")
        return # Se não houver dados, para a execução aqui

    # Renomeação do Resumo Total
    df_total.rename(columns={
        'nomeusuario': 'Usuário',
        'vl_saldototal': 'Saldo Total'
    }, inplace=True)
    
    # Função de estilo (reutilizando a lógica de cor)
    def color_saldo(val):
        color = 'red' if val < 0 else 'green' if val > 0 else 'black'
        return f'color: {color}'

    # Exibição do Resumo Total (Usando .style.format para formatar sem o R$)
    if not df_total.empty:
        st.dataframe(
            df_total.style.map(  # <--- MUDANÇA AQUI: applymap virou map
                color_saldo, 
                subset=['Saldo Total'] 
            ).format({
                'Saldo Total': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            }),
            use_container_width=True
        )

    st.markdown("---")

    # ----------------------------------------------------------------------
    # 2. TABELA CONSOLIDADO MENSAL (vw_acertomensal)
    # ----------------------------------------------------------------------
    st.subheader("Saldo Consolidado Mensal")
    df_resumo = consultar_dados("vw_acertomensal")

    if df_resumo.empty:
        st.info("Nenhuma transação para rateio pendente. Cadastre uma transação dividida ou marque as transações antigas como saldadas.")
        return

    # Renomeação do Resumo
    df_resumo.rename(columns={
        'cd_quemdeve': 'Usuário',
        'ano' : 'Ano',
        'mes' : 'Mês',
        'vl_saldoacertomensal': 'Saldo Líquido'
    }, inplace=True)

    # 💡 INCLUSÃO AQUI: Ordena o DataFrame por Ano (ASC) e Mês (ASC)
    df_resumo.sort_values(by=['Ano', 'Mês'], inplace=True)

    # Função de estilo para o Resumo (CORRIGIDO A SINTAXE E ESPERA O VALOR NUMÉRICO)
    def color_saldo_resumo(val):
        # Garante que val é um número
        if isinstance(val, str):
            try:
                # Lida com o formato brasileiro para conversão
                val = float(val.replace('.', '').replace(',', '.'))
            except ValueError:
                val = 0
                
        color = 'red' if val < 0 else 'green' if val > 0 else 'black'
        return f'color: {color}'

    # Exibição do Resumo (Usando .style.format para formatar sem o R$)
    st.dataframe(
        df_resumo.style.map(
            color_saldo_resumo, 
            subset=['Saldo Líquido'] # Aplica a cor na coluna numérica
        ).format({
        # 💡 CORREÇÃO 1: Formata 'Ano' como inteiro (sem decimais)
        'Ano': "{:.0f}",
        # 💡 CORREÇÃO 2: Formata 'Mês' como inteiro (sem decimais)
        'Mês': "{:.0f}", 
        # Formatação para xx.xxx,xx
        'Saldo Líquido': lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        }),
        column_order=['Ano', 'Mês', 'Usuário', 'Saldo Líquido'],
        use_container_width=True
    )

    st.markdown("---")

    # ----------------------------------------------------------------------
    # 3. TABELA DETALHE: Detalhe por Transação (vw_AcertoTransacao)
    # ----------------------------------------------------------------------
    st.subheader("Detalhe das Transações Pendentes de Acerto")
    
    # Usando o nome da View que você indicou: vw_acertodetalhe
    df_detalhe = consultar_dados("vw_acertodetalhe") 

    # Renomeação do Detalhe
    df_detalhe.rename(columns={
        'dt_datatransacao': 'Data',
        'dsc_transacao': 'Descrição',
        'vl_totaltransacao': 'Total da Transação',
        'cd_quempagou': 'Pagador',
        'cd_quemdeve': 'Usuário',
        'vl_proporcional': 'Devido (Parte Dele)',
        'vl_acertotransacao': 'Acerto Líquido'
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
        df_detalhe.style.map(
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

def atualizar_status_acerto(lista_ids):
    conn = None
    # Verifica se há IDs para evitar erro SQL e trabalho desnecessário
    if not lista_ids:
        return True 

    # 💡 Query usa UNNEST para desempacotar a lista de IDs do Python em valores SQL
    sql_update = """
        UPDATE stg_transacoes SET
            cd_foidividido = 'S'
        WHERE id_transacao IN (SELECT unnest(%s));
    """
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # O argumento é uma tupla contendo a lista (array) de IDs
        cursor.execute(sql_update, (lista_ids,))
        conn.commit()
        
        return True
        
    except psycopg2.Error as ex:
        # st.error deve estar acessível se essa função for chamada em um contexto Streamlit
        st.error(f"Erro do banco de dados ao realizar acerto múltiplo: {ex}")
        if conn: conn.rollback()
        return False
        
    except Exception as e:
        st.error(f"Erro inesperado ao realizar acerto múltiplo: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

def acerto_multiplo_transacoes():
    st.title("💰 Acerto de Transações Pendentes")
    st.markdown("Selecione as transações que foram acertadas/saldadas para atualizar o campo **cd_foidividido** para 'S'.")

    # 1. CARREGAR DADOS PENDENTES
    # Adicionando um filtro para carregar apenas transações não acertadas ('N') e que são despesas
    # Você precisará adaptar a chamada à sua função consultar_dados
    
    # Carrega todas as transações e filtra as pendentes ('N') em Pandas.
    df_todas = consultar_dados("stg_transacoes", usar_view=False)
    if df_todas.empty or 'cd_foidividido' not in df_todas.columns:
        df_pendentes = pd.DataFrame()
    else:
        df_pendentes = df_todas[df_todas['cd_foidividido'] == 'N']

    if df_pendentes.empty:
        st.info("🎉 Não há transações pendentes de acerto (cd_foidividido = 'N').")
        return
    
    # Ordena o DataFrame pela data da transação em ordem decrescente (mais recente primeiro)
    df_pendentes = df_pendentes.sort_values(
        by='dt_datatransacao', 
        ascending=False
    ).reset_index(drop=True) # Reseta o índice para garantir que a seleção do st.dataframe (iloc) funcione corretamente

    st.subheader(f"Transações Pendentes de Acerto ({len(df_pendentes)})")

    # 2. USAR st.data_editor PARA SELEÇÃO MÚLTIPLA
    colunas_editor = ['id_transacao', 'dt_datatransacao', 'dsc_transacao', 'vl_transacao', 'cd_quempagou']
    df_exibicao = df_pendentes[colunas_editor]
    
    config = {
        "dt_datatransacao": st.column_config.DatetimeColumn("Data", format="YYYY-MM-DD"),
        "vl_transacao": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f")
    }

    # 💡 st.dataframe é o elemento correto para seleção!
    selecao_evento = st.dataframe(
        df_exibicao,
        column_config=config,
        hide_index=True,
        use_container_width=True,
        # 💡 Chave para ativação da seleção
        selection_mode="multi-row", 
        # on_select="rerun" é opcional, mas ativa o widget para interação imediata
        on_select="rerun" 
    )

    # 3. CAPTURAR OS IDs SELECIONADOS
    # 💡 A seleção é capturada diretamente do objeto retornado (selecao_evento)
    indices_selecionados = selecao_evento.selection.rows
    
    ids_selecionados = []
    if indices_selecionados:
        # Usamos .iloc para acessar as linhas do DataFrame original (df_pendentes) pela POSIÇÃO
        # O objeto de seleção retorna os índices posicionais (0, 1, 2...)
        df_selecionadas = df_pendentes.iloc[indices_selecionados]
        ids_selecionados = df_selecionadas['id_transacao'].tolist()

    # O resto do código (botão e lógica de atualização) permanece o mesmo.
    st.caption(f"**Total de transações selecionadas:** {len(ids_selecionados)}")
    
    # 4. BOTÃO DE AÇÃO
    if st.button(f"✅ Acertar {len(ids_selecionados)} Transações Selecionadas"):
        if not ids_selecionados:
            st.warning("Selecione pelo menos uma transação para acertar.")
        else:
            with st.spinner(f"Atualizando {len(ids_selecionados)} transações..."):
                sucesso = atualizar_status_acerto(ids_selecionados)

                if sucesso:
                    st.success(f"🎉 {len(ids_selecionados)} transações foram acertadas com sucesso!")
                    # Limpar o cache para que a lista de pendentes seja atualizada
                    consultar_dados.clear()
                    st.rerun()
                else:
                    st.error("Falha ao atualizar o status de acerto no banco de dados.")

def pagina_acerto_controle():
    st.title("💰 Gestão de Acertos e Rateio")

    # 💡 Usamos st.tabs para organizar as funcionalidades
    tab_detalhe, tab_acerto_multiplo = st.tabs(["📊 Detalhe e Rateio de Contas", "✅ Acerto Múltiplo"])

    # --- ABA 1: Fluxo Original de Detalhe/Rateio ---
    with tab_detalhe:
        # Chama a função que você já tem para mostrar o rateio e detalhes
        # Se for a função original, chame-a aqui.
        exibir_detalhe_rateio() # Supondo que você tem esta função
        # Se você ainda não tem, substitua pela sua lógica de detalhe e rateio
        
    # --- ABA 2: Novo Fluxo de Acerto Múltiplo ---
    with tab_acerto_multiplo:
        # Chama a nova função que criamos na resposta anterior
        acerto_multiplo_transacoes()

def buscar_transacao_por_id(id_transacao):
    conn = None
    df_transacao = pd.DataFrame()
    
    # 1. Tabela stg_transacoes em minúsculo
    tabela = 'stg_transacoes'
    
    # 2. Uso do placeholder %s para PostgreSQL
    sql_query = f"""
        SELECT * FROM {tabela} 
        WHERE id_transacao = %s
    """
    
    try:
        conn = get_connection()
        df_transacao = pd.read_sql(sql_query, conn, params=(id_transacao,))
        
    except (psycopg2.Error, Exception) as e: 
        st.error(f"Erro ao buscar transação por ID: {e}")
        # df_transacao permanece o DataFrame vazio inicializado acima.

    finally:
        if conn:
            conn.close()
            
    # Retorna um DataFrame (1 linha ou vazio)
    return df_transacao

def atualizar_transacao_por_id(
    id_transacao, 
    dt_datatransacao, 
    id_tipotransacao, 
    dsc_tipotransacao, 
    id_categoria, 
    dsc_categoriatransacao, 
    id_subcategoria, 
    dsc_subcategoriatransacao, 
    id_usuario, 
    dsc_nomeusuario, 
    dsc_transacao, 
    vl_transacao, 
    cd_quempagou, 
    cd_edividido, 
    cd_foidividido
):
    conn = None
    
    # Tabela em minúsculo
    tabela = 'stg_transacoes'
    
    # Ajuste do SQL: Nomes de colunas em minúsculo e placeholders %s
    sql_update = f"""
        UPDATE {tabela} SET
            dt_datatransacao = %s,
            id_tipotransacao = %s,
            dsc_tipotransacao = %s,
            id_categoria = %s,
            dsc_categoriatransacao = %s,
            id_subcategoria = %s,
            dsc_subcategoriatransacao = %s,
            id_usuario = %s,
            dsc_nomeusuario = %s,
            dsc_transacao = %s,
            vl_transacao = %s,
            cd_quempagou = %s,
            cd_edividido = %s,
            cd_foidividido = %s
        WHERE id_transacao = %s; 
    """
    
    # Tupla de Valores: Inclui todos os campos na ordem do SQL, 
    # e o ID no final para a condição WHERE
    valores = (
        dt_datatransacao, 
        id_tipotransacao, 
        dsc_tipotransacao, 
        id_categoria, 
        dsc_categoriatransacao, 
        id_subcategoria, 
        dsc_subcategoriatransacao, 
        id_usuario, 
        dsc_nomeusuario, 
        dsc_transacao, 
        vl_transacao, 
        cd_quempagou, 
        cd_edividido, 
        cd_foidividido,
        id_transacao # <<< O valor do WHERE (o último %s)
    )

    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Execução: Passa o SQL e a tupla de valores
        cursor.execute(sql_update, valores)
        conn.commit()
        return True
        
    except psycopg2.Error as ex: # <<< CORREÇÃO DO DRIVER
        st.error(f"Erro do banco de dados ao atualizar transação: {ex}")
        if conn: conn.rollback()
        return False
        
    except Exception as e:
        st.error(f"Erro inesperado ao atualizar transação: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

def exibir_formulario_edicao(id_transacao):
    st.subheader(f"2. Editando Transação ID: {id_transacao}")

    # 1. BUSCAR DADOS ATUAIS DA TRANSAÇÃO
    dados_atuais = buscar_transacao_por_id(id_transacao)
    
    # 💡 CORREÇÃO: Verificação única. Se o DataFrame veio vazio, sai.
    if dados_atuais.empty: 
        st.error(f"Não foi possível carregar os dados da transação com ID {id_transacao} ou a transação não foi encontrada.")
        return # Sai da função
    
    # 2. BUSCAR DADOS PARA OS DROPDOWNS (DIMENSÕES)
    
    # Usuários (dim_usuario) - Para o dropdown "Quem Pagou"
    df_usuarios = consultar_dados("dim_usuario", usar_view=False)
    usuarios_nomes = df_usuarios['dsc_nome'].tolist() if not df_usuarios.empty and 'dsc_nome' in df_usuarios.columns else []
    
    # Categorias (dim_categoria)
    df_categorias = consultar_dados("dim_categoria", usar_view=False)
    categorias_nomes = df_categorias['dsc_categoriatransacao'].tolist() if not df_categorias.empty and 'dsc_categoriatransacao' in df_categorias.columns else []

    # Subcategorias (dim_subcategoria)
    df_subcategorias = consultar_dados("dim_subcategoria", usar_view=False)
    subcategorias_nomes = df_subcategorias['dsc_subcategoriatransacao'].tolist() if not df_subcategorias.empty and 'dsc_subcategoriatransacao' in df_subcategorias.columns else []
    
    
    # 3. PREPARAR VALORES PADRÃO
    
    # 💡 Extração única dos dados para um acesso mais limpo e seguro
    dados_atuais_scalar = dados_atuais.iloc[0]
    
    # Acesso seguro ao valor escalar da data
    data_transacao_valor = dados_atuais_scalar['dt_datatransacao']

    # Conversão segura para o st.date_input
    data_atual_dt = data_transacao_valor.date() if isinstance(data_transacao_valor, datetime.datetime) else data_transacao_valor

    # O Tipo de Transação deve usar uma lista fixa (Receita/Despesa)
    tipos_transacao = ['Despesas', 'Receitas'] # Use a sua lista real
    
    # 4. FORMULÁRIO PRÉ-PREENCHIDO
    with st.form("edicao_transacao_form"):
        
        # LINHA 1: Data, Tipo, Usuário (Quem Registrou)
        col1, col2, col3 = st.columns(3)
        
        with col1:
            nova_data = st.date_input("Data da Transação:", value=data_atual_dt)
            
        with col2:
            novo_tipo = st.selectbox("Tipo de Transação:", 
                                     tipos_transacao, 
                                     # 💡 Usando o valor escalar
                                     index=tipos_transacao.index(dados_atuais_scalar['dsc_tipotransacao']))

        with col3:
            # 💡 Usando o valor escalar
            novo_usuario_registro = st.text_input("Usuário (Quem Registrou):", 
                                                  value=dados_atuais_scalar['dsc_nomeusuario'], # Corrigido para dsc_nomeusuario
                                                  disabled=True) 

        # LINHA 2: Categoria, Subcategoria, Valor
        col4, col5, col6 = st.columns(3)
        with col4:
            categoria_atual = dados_atuais_scalar['dsc_categoriatransacao']
            if categoria_atual not in categorias_nomes:
                 categorias_nomes.append(categoria_atual)

            nova_categoria = st.selectbox("Categoria:", 
                                         categorias_nomes, 
                                         index=categorias_nomes.index(categoria_atual))

        with col5:
            subcategoria_atual = dados_atuais_scalar['dsc_subcategoriatransacao']
            if subcategoria_atual not in subcategorias_nomes:
                 subcategorias_nomes.append(subcategoria_atual)

            novo_subcategoria = st.selectbox("Subcategoria:", 
                                             subcategorias_nomes, 
                                             index=subcategorias_nomes.index(subcategoria_atual))
            
        with col6:
            # 💡 Usando o valor escalar e conversão para float
            novo_valor = st.number_input("Valor da Transação:", 
                                         value=float(dados_atuais_scalar['vl_transacao']), 
                                         min_value=0.01, 
                                         format="%.2f")
        
        # LINHA 3: Descrição Detalhada
        # 💡 Usando o valor escalar
        nova_descricao = st.text_area("Descrição Detalhada:", 
                                      value=dados_atuais_scalar['dsc_transacao'])

        # LINHA 4: Controle de Pagamento
        st.markdown("##### Controle de Pagamento")
        col7, col8, col9 = st.columns(3)
        
        with col7:
            pagador_atual = dados_atuais_scalar['cd_quempagou']
            if pagador_atual not in usuarios_nomes:
                 usuarios_nomes.append(pagador_atual)

            novo_pagador = st.selectbox("Quem Pagou (Nome/Apelido):", 
                                         usuarios_nomes, 
                                         index=usuarios_nomes.index(pagador_atual))

        with col8:
            opcoes_divisao = ('N', 'S')
            novo_e_dividido = st.radio("Essa transação será dividida?", 
                                         opcoes_divisao, 
                                         index=opcoes_divisao.index(dados_atuais_scalar['cd_edividido']), 
                                         horizontal=True)
        
        with col9:
            opcoes_acerto = ('N', 'S')
            novo_foi_dividido = st.radio("A transação foi acertada/saldada?", 
                                         opcoes_acerto, 
                                         index=opcoes_acerto.index(dados_atuais_scalar['cd_foidividido']), 
                                         horizontal=True)

        submitted = st.form_submit_button("Salvar Correção")

        if submitted:
            # 💡 1. Lógica para buscar os IDs necessários a partir das descrições (Lookups)
            
            # id_tipotransacao (Assumindo que 1=Despesas, 2=Receitas)
            # Este já é um int nativo
            id_tipo = 1 if novo_tipo == 'Despesas' else 2
            
            # id_categoria
            # CORREÇÃO: Conversão para int() nativo
            id_categoria = int(df_categorias[
                df_categorias['dsc_categoriatransacao'] == nova_categoria
            ]['id_categoria'].iloc[0])
            
            # id_subcategoria
            # CORREÇÃO: Conversão para int() nativo
            id_subcategoria = int(df_subcategorias[
                df_subcategorias['dsc_subcategoriatransacao'] == novo_subcategoria
            ]['id_subcategoria'].iloc[0])
            
            # id_usuario
            # CORREÇÃO: Conversão para int() nativo
            id_usuario = int(df_usuarios[
                df_usuarios['dsc_nome'] == novo_usuario_registro 
            ]['id_usuario'].iloc[0])

            # dsc_nomeusuario
            dsc_nomeusuario = novo_usuario_registro
            
            
            # 💡 2. CHAMADA CORRIGIDA: Os argumentos agora são tipos nativos
            sucesso = atualizar_transacao_por_id(
                id_transacao,                  
                nova_data,                     
                id_tipo,                       
                novo_tipo,                     
                id_categoria,                  
                nova_categoria,                
                id_subcategoria,               
                novo_subcategoria,             
                id_usuario,                    
                dsc_nomeusuario,               
                nova_descricao,                
                novo_valor,                    
                novo_pagador,                  
                novo_e_dividido,               
                novo_foi_dividido              
            )
            
            if sucesso:
                st.success(f"Transação {id_transacao} atualizada com sucesso!")
                consultar_dados.clear()
                st.rerun() 
            else:
                st.error("Erro ao atualizar a transação. Verifique a conexão com o banco.")

def editar_transacao():
    st.header("Correção de Transações")

    # ----------------------------------------------------------------------
    # A) TABELA DE VISUALIZAÇÃO
    # ----------------------------------------------------------------------
    st.subheader("1. Tabela de Transações Registradas")

    # Consulta a tabela de transações (stg_transacoes já deve trazer 'dt_datatransacao')
    df_transacoes = consultar_dados("stg_transacoes")
    
    if df_transacoes.empty:
        st.info("Nenhuma transação registrada para editar.")
        return

    # 💡 LÓGICA DE FILTRO DE DATA (INÍCIO)
    # 1. Calcula o primeiro dia do mês anterior
    hoje = datetime.date.today()
    primeiro_dia_mes_anterior = hoje - relativedelta(months=1)
    primeiro_dia_mes_anterior = primeiro_dia_mes_anterior.replace(day=1)
    
    # 2. Converte a coluna de data para datetime (se ainda não for)
    # Garante que a coluna de data seja comparável
    df_transacoes['dt_datatransacao'] = pd.to_datetime(df_transacoes['dt_datatransacao'])
    
    # 3. Filtra o DataFrame
    df_filtrado = df_transacoes[
        (df_transacoes['dt_datatransacao'].dt.date >= primeiro_dia_mes_anterior) & 
        (df_transacoes['cd_foidividido'] == 'N')
    ]
    
    # Se o DataFrame filtrado estiver vazio
    if df_filtrado.empty:
        st.info(f"Nenhuma transação encontrada a partir de {primeiro_dia_mes_anterior.strftime('%d/%m/%Y')}.")
        return
    # 💡 FIM DA LÓGICA DE FILTRO
    
    # Renomeação simplificada para o usuário escolher (Usando o DF FILTRADO)
    df_exibicao = df_filtrado.rename(columns={
        'id_transacao': 'ID', # Mantenha o ID visível e em primeiro
        'dt_datatransacao': 'Data',
        'dsc_transacao': 'Descrição',
        'vl_transacao': 'Valor',
        'cd_quempagou': 'Pagador'
    })[['ID', 'Data', 'Descrição', 'Valor', 'Pagador', 'cd_edividido', 'cd_foidividido']]

    # Exibe a tabela COMPLETA, apenas para visualização (sem modo de seleção)
    st.dataframe(
        df_exibicao, 
        hide_index=True, 
        use_container_width=True
    )
    
    st.markdown("---")
    
    # ----------------------------------------------------------------------
    # B) CAMPO DE SELEÇÃO MANUAL
    # ----------------------------------------------------------------------
    st.subheader("2. Insira o ID para Editar")
    
    # Lista de IDs disponíveis para seleção no campo (usando o DF FILTRADO)
    lista_ids = [''] + df_exibicao['ID'].astype(str).tolist()
    
    # Permite que o usuário selecione o ID
    id_selecionado_str = st.selectbox(
        "Selecione o ID da transação que deseja corrigir na lista acima:",
        options=lista_ids,
        index=0 # Começa com vazio
    )

    # ----------------------------------------------------------------------
    # C) LÓGICA DE CARREGAMENTO DO FORMULÁRIO
    # ----------------------------------------------------------------------
    
    if id_selecionado_str and id_selecionado_str != '':
        try:
            # Converte o ID para o tipo numérico (presumivelmente int, como no seu DB)
            id_transacao_selecionada = int(id_selecionado_str) 
            
            # Chama a função para exibir o formulário de edição
            exibir_formulario_edicao(id_transacao_selecionada)
        
        except ValueError:
            st.error("Erro: O ID selecionado não é um número válido.")
    else:
        st.info("O formulário de edição aparecerá aqui após a seleção do ID.")

def deletar_registro_dimensao(tabela, id_coluna, id_registro):
    conn = None

    # 1. Garante que o nome da tabela e a coluna de ID estão em minúsculo
    tabela_lower = tabela.lower()
    id_coluna = id_coluna.lower()

    # 2. Uso do placeholder %s e injeção do nome da tabela e da coluna de ID
    sql_delete = f"""
        DELETE FROM {tabela_lower}
        WHERE {id_coluna} = %s;
    """
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # 3. Execução: Passa o ID como uma tupla
        cursor.execute(sql_delete, (id_registro,)) 
        
        conn.commit()
        return True
        
    except psycopg2.Error as ex: # <<< CORREÇÃO DO DRIVER
        st.error(f"Erro do banco de dados ao deletar em {tabela_lower}: {ex}")
        if conn: conn.rollback()
        return False
        
    except Exception as e:
        st.error(f"Erro inesperado: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

def atualizar_registro_dimensao(tabela, id_coluna, id_registro, campos_valores):
    conn = None

    # 1. Garante que o nome da tabela e a coluna de ID estão em minúsculo
    tabela_lower = tabela.lower()
    id_coluna = id_coluna.lower()

    # 2. Constrói a string SET a partir do dicionário {coluna: valor}
    #    Ex: {'dsc_categoriatransacao': 'Lazer'} -> 'dsc_categoriatransacao = %s'
    colunas = list(campos_valores.keys())
    valores = list(campos_valores.values())
    set_clause_str = ", ".join(f"{coluna.lower()} = %s" for coluna in colunas)

    # 3. Constrói o SQL de UPDATE
    sql_update = f"""
        UPDATE {tabela_lower} SET
            {set_clause_str}
        WHERE {id_coluna} = %s;
    """

    # 4. Constrói a tupla de valores: (valores_a_atualizar) + (id_registro)
    # A tupla de valores deve ser a lista de novos valores, seguida pelo ID para o WHERE
    valores_com_id = valores + [id_registro]

    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Execução: Passa o SQL e a tupla de valores
        # O psycopg2 faz o bind dos %s com os valores na ordem
        cursor.execute(sql_update, valores_com_id) 
        conn.commit()
        return True
        
    except psycopg2.Error as ex: # <<< CORREÇÃO DO DRIVER
        st.error(f"Erro do banco de dados ao atualizar em {tabela_lower}: {ex}")
        if conn: conn.rollback()
        return False
        
    except Exception as e:
        st.error(f"Erro inesperado: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

if 'menu_selecionado' not in st.session_state:
    st.session_state.menu_selecionado = "Registrar Transação"

def autenticar_usuario(login, senha):
    """
    Verifica se o login e a senha correspondem a um registro em dim_usuario.
    Retorna id_usuario, nome_completo e login em caso de sucesso.
    """
    conn = None
    usuario_info = {}
    try:
        # A função get_connection() deve estar definida em outro lugar do seu main.py
        conn = get_connection()
        cursor = conn.cursor()
        
        # 💡 CORREÇÃO DA QUERY: Seleciona id_usuario, dsc_nome e login.
        # A ordem da seleção deve ser refletida no mapeamento abaixo.
        sql = "SELECT id_usuario, dsc_nome, login FROM dim_usuario WHERE login = %s AND senha = %s;"
        
        # O placeholder %s é apropriado para PostgreSQL/Psycopg2 ou MySQL/MySQL Connector.
        cursor.execute(sql, (login, senha))
        
        resultado = cursor.fetchone() 
        
        if resultado:
            # 💡 MAPEAMENTO CORRETO: Posições do resultado da query
            # resultado[0] -> id_usuario
            # resultado[1] -> dsc_nome
            # resultado[2] -> login
            usuario_info['id_usuario'] = resultado[0]      # ID (chave que faltava)
            usuario_info['nome_completo'] = resultado[1]   # dsc_nome
            usuario_info['login'] = resultado[2]           # login
            
    except Exception as e:
        # Erro de conexão/autenticação
        st.error("Ocorreu um erro na autenticação. Verifique a conexão com o banco de dados e as credenciais.")
        print(f"Erro de autenticação: {e}")
        usuario_info = {} # Garante que retorne um dicionário vazio em caso de falha
    finally:
        if conn:
            conn.close()
            
    return usuario_info # Retorna o dicionário com as informações do usuário ou {}

def login_page():
    
    # CRÍTICO: Cria três colunas para centralizar o formulário
    # [Esquerda (espaço vazio), Centro (formulário), Direita (espaço vazio)]
    # A coluna central com o valor 0.6 garantirá que o formulário seja estreito.
    col_vazia1, col_form, col_vazia2 = st.columns([1, 0.6, 1])
    
    # Tudo o que está relacionado ao login agora deve ser colocado na coluna central
    with col_form:
        st.title("Acesso ao Sistema")
        
        with st.form("login_form"):
            login_input = st.text_input("Login (Usuário)", key="login_input_key") 
            senha_input = st.text_input("Senha", type="password", key="senha_input_key")
            
            submitted = st.form_submit_button("Entrar", use_container_width=True)
            
            if submitted:
                conn = None
                try:
                    conn = get_connection() 
                    cursor = conn.cursor()
                    
                    # Consulta para obter id_usuario e dsc_nome (nome completo)
                    query = sql.SQL("SELECT id_usuario, dsc_nome FROM dim_usuario WHERE login = %s AND senha = %s")
                    cursor.execute(query, (login_input, senha_input))
                    
                    user_data = cursor.fetchone()
                    
                    if user_data:
                        st.session_state.logged_in = True
                        st.session_state.id_usuario_logado = user_data[0]
                        st.session_state.login = login_input
                        st.session_state.nome_completo = user_data[1] 
                        st.session_state.menu_selecionado = "Dashboard"
                        st.success(f"Bem-vindo, {user_data[1]}! Acesso concedido.")
                        st.rerun()
                    else:
                        st.error("Login ou senha incorretos. Tente novamente.")
                        
                except Exception as e:
                    st.error(f"Erro ao tentar conectar ou consultar o banco de dados. Verifique a conexão e as credenciais: {e}")
                finally:
                    if conn is not None:
                        conn.close()

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

# -----------------------------------------------------------------
# FUNÇÃO AUXILIAR PARA PROJETAR DADOS FUTUROS
# -----------------------------------------------------------------
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

# -----------------------------------------------------------------
# FUNÇÃO PRINCIPAL DO DASHBOARD
# -----------------------------------------------------------------
def dashboard():
    st.title("📊 Dashboard Financeiro")
    
    # 1. CONSULTA DE DADOS
    try:
        df_transacoes = consultar_dados("stg_transacoes") 
        df_salario = consultar_dados("fact_salario")
        
    except Exception as e:
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
    
    # -----------------------------------------------------------------
    # FILTRO DE TEMPO
    # -----------------------------------------------------------------
    today = datetime.date.today()
    
    # 1. VISÃO PASSADA (13 meses: OUT/2024 até o mês atual - Out/2025)
    start_date_passado = today.replace(day=1) - relativedelta(months=12)
    end_limit_passado = today.replace(day=1) + relativedelta(months=1) 
    
    meses_passado = [
        (today.replace(day=1) - relativedelta(months=i)).strftime('%Y-%m')
        for i in range(12, -1, -1)
    ]
    
    df_passado_saldo = df_dados_mensais[df_dados_mensais['ano_mes'].isin(meses_passado)].copy()
    
    # 2. VISÃO FUTURA (12 meses: Próximo mês - Nov/2025 até Out/2026)
    start_date_futuro = today.replace(day=1) + relativedelta(months=1)
    end_date_futuro = start_date_futuro + relativedelta(months=12)
    
    meses_futuro = [
        (start_date_futuro + relativedelta(months=i)).strftime('%Y-%m')
        for i in range(12) 
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
            fig3 = criar_grafico_saldo_combinado(df_saldo_passado_final, 'Receitas, Despesas e Saldo (Últimos 13 Meses)')
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Dados de balanço insuficientes no período passado.")

    with col_saldo_futuro:
        st.subheader("Projeção de Balanço (Futuro)")
        if not df_saldo_futuro_final.empty:
            fig4 = criar_grafico_saldo_combinado(df_saldo_futuro_final, 'Projeção de Balanço (Próximos 12 Meses)')
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
                title='Passado (Últimos 13 Meses)',
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
                title='Futuro (Próximos 12 Meses)',
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
                title='Passado (Últimos 13 Meses)',
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
                title='Futuro (Próximos 12 Meses)',
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

def main():
    # Inicializa o estado de login
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'menu_selecionado' not in st.session_state:
        st.session_state.menu_selecionado = "Dashboard"
        
    # ----------------------------------------------------------------
    # CONTROLE DE FLUXO: Se não estiver logado, exibe apenas a tela de login
    # ----------------------------------------------------------------
    if not st.session_state.logged_in:
        login_page()
        return 
    
    # Se estiver logado, continua a execução do menu
    
    # --- 1. SIDEBAR (Menu Principal) ---
    with st.sidebar:
        # ALTERAÇÃO 1: Usar dsc_nome (nome_completo) em vez de login
        nome_exibido = st.session_state.get('nome_completo', st.session_state.login)
        st.title(f"Menu Principal - Logado como: {nome_exibido}")
        
        # BOTÕES PRINCIPAIS (Dashboard, Transação, Acerto, Corrigir) - 2 colunas
        col1, col2 = st.columns(2) 

        with col1:
            if st.button("📊 Dashboard", key="btn_dashboard", use_container_width=True):
                st.session_state.menu_selecionado = "Dashboard"
        with col2:
            if st.button("💵 Transação", key="btn_transacao", use_container_width=True):
                st.session_state.menu_selecionado = "Transação"

        col3, col4 = st.columns(2)
        
        with col3:
            if st.button("💰 Acerto", key="btn_acerto", use_container_width=True):
                st.session_state.menu_selecionado = "Acerto de Contas"
        with col4:
            if st.button("🛠️ Corrigir", key="btn_corrigir", use_container_width=True):
                st.session_state.menu_selecionado = "Corrigir Transação"
        
        st.subheader("Cadastros (Dimensões)")
        
        # Dicionário de Opções
        opcoes_cadastro = {
            "💳 Tipos de Transação": "Tipos de Transação", 
            "🏷️ Categorias": "Categorias",        
            "📝 Subcategorias": "Subcategorias",     
            "👥 Usuários": "Usuários",          
            "💰 Salário": "Salário",
        }
        
        # ALTERAÇÃO 2: Layout de 2 botões por linha para Cadastros
        opcoes_lista = list(opcoes_cadastro.keys())
        
        for i in range(0, len(opcoes_lista), 2):
            col_a, col_b = st.columns(2)
            
            # Botão da coluna A
            nome_opcao_a = opcoes_lista[i]
            nome_limpo_a = opcoes_cadastro[nome_opcao_a]
            
            with col_a:
                if st.button(nome_opcao_a, key=f"btn_cadastro_{nome_limpo_a}", use_container_width=True):
                    st.session_state.menu_selecionado = nome_limpo_a

            # Botão da coluna B (se existir)
            if i + 1 < len(opcoes_lista):
                nome_opcao_b = opcoes_lista[i+1]
                nome_limpo_b = opcoes_cadastro[nome_opcao_b]
                
                with col_b:
                    if st.button(nome_opcao_b, key=f"btn_cadastro_{nome_limpo_b}", use_container_width=True):
                        st.session_state.menu_selecionado = nome_limpo_b
        
        # Botões de Logout/Cache
        st.markdown("---")
        
        if st.button("♻️ Limpar Cache", on_click=limpar_cache_dados, use_container_width=True):
             pass 
        
        # ALTERAÇÃO 3: Estilo CSS específico para o botão "🛑 Sair"
        st.markdown(
            """
            <style>
            /* Seleciona APENAS o penúltimo elemento stButton na sidebar (que é o botão Sair) */
            [data-testid='stSidebar'] div.stButton:nth-last-child(2) > button {
                background-color: #ff4b4b; /* Vermelho padrão de erro/Streamlit */
                color: white;
                border-color: #ff4b4b;
            }
            </style>""", unsafe_allow_html=True
        )
        
        if st.button("🛑 Sair", key="btn_logout", use_container_width=True):
            st.session_state.logged_in = False
            # Limpa as variáveis de sessão sensíveis
            if 'id_usuario_logado' in st.session_state:
                 del st.session_state.id_usuario_logado
            if 'login' in st.session_state:
                 del st.session_state.login
            if 'nome_completo' in st.session_state:
                 del st.session_state.nome_completo
            st.rerun() 

    # --- 2. EXIBIÇÃO DO FORMULÁRIO SELECIONADO ---
    opcao_atual = st.session_state.menu_selecionado
    
    # As funções abaixo devem estar definidas no seu código
    if opcao_atual == "Dashboard":
        dashboard()
    elif opcao_atual == "Transação":
        formulario_transacao()
    elif opcao_atual == "Salário":
        formulario_salario()
    elif opcao_atual == "Corrigir Transação": 
        editar_transacao()
    elif opcao_atual == "Acerto de Contas":
        pagina_acerto_controle()
    elif opcao_atual == "Tipos de Transação":
        formulario_tipo_transacao()
    elif opcao_atual == "Categorias":
        formulario_categoria()
    elif opcao_atual == "Subcategorias":
        formulario_subcategoria()
    elif opcao_atual == "Usuários":
        formulario_usuario()

# ESTE BLOCO É O MAIS CRÍTICO: CHAMA A FUNÇÃO main() PARA INICIAR O APP
if __name__ == "__main__":
    main()