"""Formulários de cadastro/edição e telas de acerto."""
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd
import streamlit as st
from helpers import cor_saldo, formatar_moeda, logger
from db import atualizar_registro_dimensao, atualizar_status_acerto, atualizar_transacao_por_id, buscar_transacao_por_id, consultar_dados, deletar_registro_dimensao, deletar_transacoes, inserir_dados

def _bloco_confirmacao_exclusao(chave_id, chave_nome, mensagem_aviso, fn_deletar):
    id_del = st.session_state.get(chave_id)
    if not id_del:
        return
    nome_del = st.session_state.get(chave_nome, "")
    st.markdown("---")
    st.error(f"⚠️ CONFIRMAÇÃO DE EXCLUSÃO: {mensagem_aviso.format(nome=nome_del, id=id_del)}")
    col_sim, col_nao = st.columns(2)
    with col_sim:
        if st.button("SIM, EXCLUIR PERMANENTEMENTE", key=f"del_sim_{chave_id}"):
            if fn_deletar(id_del):
                st.success(f"Registro ID {id_del} excluído com sucesso.")
                st.session_state[chave_id] = None
                st.rerun()
    with col_nao:
        if st.button("CANCELAR Exclusão", key=f"del_nao_{chave_id}"):
            st.session_state[chave_id] = None
            st.rerun()

def reset_categoria():
    """Reseta a Categoria e Subcategoria ao mudar o Tipo de Transação."""
    # Define a chave de Categoria para o primeiro valor (index=0)
    # A Subcategoria é implicitamente reajustada no próximo re-run.
    if 'sel_cat' in st.session_state:
        st.session_state.sel_cat = None
    if 'sel_sub' in st.session_state:
        st.session_state.sel_sub = None

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
    # B) VISUALIZAÇÃO E SELEÇÃO MANUAL
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
            logger.exception("Erro ao carregar dados do Tipo de Transação")
            st.error(f"Erro ao carregar dados do ID: {e}")


    _bloco_confirmacao_exclusao(
        'confirm_delete_id_tipo',
        'confirm_delete_nome_tipo',
        "Tem certeza que deseja EXCLUIR o tipo '{nome}' (ID {id})? Esta ação é irreversível e pode causar erros de integridade em Transações.",
        lambda id_del: deletar_registro_dimensao("dim_tipotransacao", "id_tipotransacao", id_del),
    )

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
    # As chaves do dicionário são em minúsculo
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
            logger.exception("Erro ao carregar dados da Categoria")
            st.error(f"Erro ao carregar dados do ID. Detalhe: {e}")


    _bloco_confirmacao_exclusao(
        'confirm_delete_id_cat',
        'confirm_delete_nome_cat',
        "Tem certeza que deseja EXCLUIR a Categoria '{nome}' (ID {id})? Esta ação é irreversível e impedirá a exclusão se houver Subcategorias ou Transações vinculadas.",
        lambda id_del: deletar_registro_dimensao("dim_categoria", "id_categoria", id_del),
    )

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

    # Usa a view vw_dim_subcategoria
    df_subcategorias = consultar_dados("vw_dim_subcategoria") 

    if df_subcategorias.empty:
        st.info("Nenhuma subcategoria registrada.")
        return

    # Mapeia as chaves em minúsculo
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
            logger.exception("Erro ao carregar dados da Subcategoria")
            st.error(f"Erro ao carregar dados do ID. Verifique se o ID existe ou se os nomes das colunas da View estão corretos: {e}")


    _bloco_confirmacao_exclusao(
        'confirm_delete_id_sub',
        'confirm_delete_nome_sub',
        "Tem certeza que deseja EXCLUIR a Subcategoria '{nome}' (ID {id})? Esta ação é irreversível.",
        lambda id_del: deletar_registro_dimensao("dim_subcategoria", "id_subcategoria", id_del),
    )

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
        df_usuarios = consultar_dados("dim_usuario")
    except Exception:
        logger.exception("Falha ao carregar usuários no formulário de salário")
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

                st.success(f"Salário de {valor_salario:.2f} € registrado para {usuario_selecionado_nome}!")
            else:
                st.warning("O Valor do Salário deve ser maior que zero.")
    # ------------------ FIM FORMULÁRIO ------------------


    # Exibe a tabela com as colunas ajustadas, usando a View
    st.subheader("Salários Registrados")

    # 1. Consulta a View que já tem o Nome do Usuário, Ano e Mês
    df_salarios = consultar_dados("vw_fact_salarios")

    if not df_salarios.empty:

        # Renomeação das Colunas para Exibição
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
        # Se precisar de formatação específica (X.XXX,XX €), use st.dataframe.
        df_exibicao['Valor do Salário'] = df_exibicao['Valor do Salário'].apply(formatar_moeda) 

        # 6. Exibe o DataFrame com os nomes de colunas corretos
        st.dataframe(df_exibicao[colunas_finais], hide_index=True, use_container_width=True)

    else:
        st.info("Nenhum salário registrado.")

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
        nome_usuario = st.session_state.nome_completo

    except AttributeError:
        st.error("Erro de Sessão: As variáveis de usuário logado (id_usuario_logado e nome_completo) não estão configuradas na sessão.")
        return

    # Exibição usa o nome completo
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
            usuario_nome_final = nome_usuario
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

    # Exibição do Resumo Total (formatado sem o símbolo €)
    if not df_total.empty:
        st.dataframe(
            df_total.style.map(
                cor_saldo,
                subset=['Saldo Total']
            ).format({
                'Saldo Total': formatar_moeda
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

    # Ordena o DataFrame por Ano e Mês (crescente)
    df_resumo.sort_values(by=['Ano', 'Mês'], inplace=True)

    # Exibição do Resumo (formatado sem o símbolo €)
    st.dataframe(
        df_resumo.style.map(
            cor_saldo,
            subset=['Saldo Líquido']
        ).format({
            'Ano': "{:.0f}",
            'Mês': "{:.0f}",
            'Saldo Líquido': formatar_moeda
        }),
        column_order=['Ano', 'Mês', 'Usuário', 'Saldo Líquido'],
        use_container_width=True
    )

    st.markdown("---")

    # ----------------------------------------------------------------------
    # 3. TABELA DETALHE: Detalhe por Transação (vw_AcertoTransacao)
    # ----------------------------------------------------------------------
    st.subheader("Detalhe das Transações Pendentes de Acerto")

    # View de detalhe: vw_acertodetalhe
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

    # Exibição do Detalhe (formatação de moeda + cor por sinal)
    st.dataframe(
        df_detalhe.style.map(
            cor_saldo,
            subset=['Acerto Líquido']
        ).format({
            'Total da Transação': formatar_moeda,
            'Devido (Parte Dele)': formatar_moeda,
            'Acerto Líquido': formatar_moeda
        }),
        use_container_width=True
    )

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
        "vl_transacao": st.column_config.NumberColumn("Valor (€)", format="%.2f €")
    }

    # st.dataframe é o elemento correto para seleção!
    selecao_evento = st.dataframe(
        df_exibicao,
        column_config=config,
        hide_index=True,
        use_container_width=True,
        # Chave para ativação da seleção
        selection_mode="multi-row", 
        # on_select="rerun" é opcional, mas ativa o widget para interação imediata
        on_select="rerun" 
    )

    # 3. CAPTURAR OS IDs SELECIONADOS
    # A seleção é capturada diretamente do objeto retornado (selecao_evento)
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

def excluir_transacoes_duplicadas():
    st.subheader("Excluir Transações")
    st.markdown("Selecione as transações duplicadas ou incorretas para excluí-las permanentemente.")

    df_todas = consultar_dados("stg_transacoes", usar_view=False)
    if df_todas.empty:
        st.info("Nenhuma transação encontrada.")
        return

    df_todas = df_todas.sort_values(by='dt_datatransacao', ascending=False).reset_index(drop=True)

    colunas_exibicao = ['id_transacao', 'dt_datatransacao', 'dsc_transacao', 'vl_transacao',
                        'dsc_categoriatransacao', 'dsc_nomeusuario', 'cd_foidividido']
    colunas_presentes = [c for c in colunas_exibicao if c in df_todas.columns]
    df_exibicao = df_todas[colunas_presentes]

    config = {
        "dt_datatransacao": st.column_config.DatetimeColumn("Data", format="YYYY-MM-DD"),
        "vl_transacao": st.column_config.NumberColumn("Valor (€)", format="%.2f €"),
        "id_transacao": st.column_config.NumberColumn("ID"),
        "dsc_transacao": st.column_config.TextColumn("Descrição"),
        "dsc_categoriatransacao": st.column_config.TextColumn("Categoria"),
        "dsc_nomeusuario": st.column_config.TextColumn("Usuário"),
        "cd_foidividido": st.column_config.TextColumn("Acertado"),
    }

    selecao = st.dataframe(
        df_exibicao,
        column_config=config,
        hide_index=True,
        use_container_width=True,
        selection_mode="multi-row",
        on_select="rerun",
    )

    indices_selecionados = selecao.selection.rows
    ids_selecionados = []
    if indices_selecionados:
        df_sel = df_todas.iloc[indices_selecionados]
        ids_selecionados = df_sel['id_transacao'].tolist()

    st.caption(f"**{len(ids_selecionados)} transação(ões) selecionada(s)**")

    if ids_selecionados:
        with st.expander("⚠️ Confirmar exclusão", expanded=True):
            st.warning(
                f"Você está prestes a excluir **{len(ids_selecionados)} transação(ões)** de forma permanente. "
                "Esta ação não pode ser desfeita."
            )
            df_confirmacao = df_todas[df_todas['id_transacao'].isin(ids_selecionados)][colunas_presentes]
            st.dataframe(df_confirmacao, column_config=config, hide_index=True, use_container_width=True)

            if st.button("🗑️ Confirmar e Excluir", type="primary"):
                with st.spinner("Excluindo transações..."):
                    sucesso = deletar_transacoes(ids_selecionados)
                if sucesso:
                    st.success(f"{len(ids_selecionados)} transação(ões) excluída(s) com sucesso!")
                    consultar_dados.clear()
                    st.rerun()

def pagina_acerto_controle():
    st.title("💰 Gestão de Acertos e Correções")

    tab_detalhe, tab_acerto_multiplo, tab_excluir, tab_corrigir = st.tabs([
        "📊 Detalhe e Rateio de Contas",
        "✅ Acerto Múltiplo",
        "🗑️ Excluir Transação",
        "🛠️ Corrigir Transação",
    ])

    with tab_detalhe:
        exibir_detalhe_rateio()

    with tab_acerto_multiplo:
        acerto_multiplo_transacoes()

    with tab_excluir:
        excluir_transacoes_duplicadas()

    with tab_corrigir:
        editar_transacao()

def exibir_formulario_edicao(id_transacao):
    st.subheader(f"2. Editando Transação ID: {id_transacao}")

    # 1. BUSCAR DADOS ATUAIS DA TRANSAÇÃO
    dados_atuais = buscar_transacao_por_id(id_transacao)

    # Verificação única. Se o DataFrame veio vazio, sai.
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

    # Tipos de Transação (dim_tipotransacao) - IDs reais vindos do banco
    df_tipos = consultar_dados("dim_tipotransacao", usar_view=False)
    tipos_map = dict(zip(df_tipos['dsc_tipotransacao'], df_tipos['id_tipotransacao'])) if not df_tipos.empty else {}


    # 3. PREPARAR VALORES PADRÃO

    # Extração única dos dados para um acesso mais limpo e seguro
    dados_atuais_scalar = dados_atuais.iloc[0]

    # Acesso seguro ao valor escalar da data
    data_transacao_valor = dados_atuais_scalar['dt_datatransacao']

    # Conversão segura para o st.date_input
    data_atual_dt = data_transacao_valor.date() if isinstance(data_transacao_valor, datetime.datetime) else data_transacao_valor

    # Lista de Tipos de Transação derivada do banco (dim_tipotransacao). Garante
    # que o tipo atual da transação esteja presente para não quebrar o selectbox
    # caso a dimensão divirja dos dados gravados.
    tipos_transacao = list(tipos_map.keys())
    tipo_atual = dados_atuais_scalar['dsc_tipotransacao']
    if tipo_atual not in tipos_transacao:
        tipos_transacao.append(tipo_atual)

    # 4. FORMULÁRIO PRÉ-PREENCHIDO
    with st.form("edicao_transacao_form"):

        # LINHA 1: Data, Tipo, Usuário (Quem Registrou)
        col1, col2, col3 = st.columns(3)

        with col1:
            nova_data = st.date_input("Data da Transação:", value=data_atual_dt)

        with col2:
            novo_tipo = st.selectbox("Tipo de Transação:",
                                     tipos_transacao,
                                     index=tipos_transacao.index(tipo_atual))

        with col3:
            # Usando o valor escalar
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
            # Usando o valor escalar e conversão para float
            novo_valor = st.number_input("Valor da Transação:", 
                                         value=float(dados_atuais_scalar['vl_transacao']), 
                                         min_value=0.01, 
                                         format="%.2f")

        # LINHA 3: Descrição Detalhada
        # Usando o valor escalar
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
            # 1. Lógica para buscar os IDs necessários a partir das descrições (Lookups)

            # id_tipotransacao (ID real obtido do banco via dim_tipotransacao)
            id_tipo = int(tipos_map[novo_tipo])

            # id_categoria
            # Conversão para int() nativo
            id_categoria = int(df_categorias[
                df_categorias['dsc_categoriatransacao'] == nova_categoria
            ]['id_categoria'].iloc[0])

            # id_subcategoria
            # Conversão para int() nativo
            id_subcategoria = int(df_subcategorias[
                df_subcategorias['dsc_subcategoriatransacao'] == novo_subcategoria
            ]['id_subcategoria'].iloc[0])

            # id_usuario
            # Conversão para int() nativo
            id_usuario = int(df_usuarios[
                df_usuarios['dsc_nome'] == novo_usuario_registro 
            ]['id_usuario'].iloc[0])

            # dsc_nomeusuario
            dsc_nomeusuario = novo_usuario_registro


            # 2. Argumentos passados como tipos nativos
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

    # LÓGICA DE FILTRO DE DATA (INÍCIO)
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
    # FIM DA LÓGICA DE FILTRO

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
