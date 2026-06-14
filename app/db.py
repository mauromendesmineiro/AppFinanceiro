"""Camada de acesso a dados (engine SQLAlchemy, pool e operações de BD)."""
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import psycopg2
import streamlit as st
from helpers import logger

@st.cache_resource
def get_engine():
    """Cria uma única vez um engine SQLAlchemy com pool de conexões (Postgres/Neon).

    O pool é compartilhado entre reruns/sessões do Streamlit (via cache_resource),
    evitando abrir uma conexão TCP nova a cada query. `pool_pre_ping` descarta
    conexões mortas (ex.: timeout do Neon) antes de reutilizá-las.
    """
    # As credenciais são carregadas do secrets.toml (bloco [postgresql])
    conn_details = st.secrets["postgresql"]
    url = URL.create(
        "postgresql+psycopg2",
        username=conn_details["username"],
        password=conn_details["password"],
        host=conn_details["server"],
        port=conn_details["port"],
        database=conn_details["database"],
    )
    return create_engine(
        url,
        connect_args={"sslmode": "require"},  # O Neon exige SSL
        pool_pre_ping=True,
        pool_recycle=600,
    )

def get_connection():
    """Retorna uma conexão psycopg2 obtida do pool do engine.

    Compatível com o uso existente (cursor/commit/rollback/close). O `.close()`
    devolve a conexão ao pool em vez de encerrar o socket.
    """
    return get_engine().raw_connection()

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

    df = pd.DataFrame()

    try:
        engine = get_engine()

        # Monta a query com o identificador citado de forma segura. O render exige
        # a conexão psycopg2 real (raw.driver_connection), e não o wrapper do pool.
        sql_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(tabela_ou_view))
        raw = engine.raw_connection()
        try:
            query_str = sql_query.as_string(raw.driver_connection)
        finally:
            raw.close()

        # Lê passando o engine SQLAlchemy (evita o UserWarning do pandas).
        df = pd.read_sql(text(query_str), engine)

    except SQLAlchemyError as e:
        logger.exception("Erro de banco ao consultar '%s'", tabela_ou_view)
        st.error(f"Erro ao consultar o banco de dados para a tabela '{tabela_ou_view}'. Detalhes: {e}")
        df = pd.DataFrame()

    except Exception as e:
        logger.exception("Erro inesperado ao consultar '%s'", tabela_ou_view)
        st.error(f"Erro inesperado ao consultar a tabela '{tabela_ou_view}'. Detalhes: {e}")
        df = pd.DataFrame()

    return df

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
        consultar_dados.clear()
        return True

    except psycopg2.Error as ex:
        logger.exception("Erro de banco ao inserir em %s", tabela_lower)
        st.error(f"Erro do banco de dados ao inserir: {ex}")
        if conn: conn.rollback()

    except Exception as e:
        logger.exception("Erro inesperado ao inserir em %s", tabela_lower)
        st.error(f"Erro inesperado: {e}")
        if conn: conn.rollback()

    finally:
        if conn: conn.close()

def buscar_transacao_por_id(id_transacao):
    df_transacao = pd.DataFrame()

    # Tabela stg_transacoes — parâmetro nomeado (:id_transacao) para o engine.
    sql_query = text("""
        SELECT * FROM stg_transacoes
        WHERE id_transacao = :id_transacao
    """)

    try:
        df_transacao = pd.read_sql(
            sql_query,
            get_engine(),
            params={"id_transacao": id_transacao},
        )

    except SQLAlchemyError as e:
        logger.exception("Erro de banco ao buscar transação por ID %s", id_transacao)
        st.error(f"Erro ao buscar transação por ID: {e}")
        # df_transacao permanece o DataFrame vazio inicializado acima.

    except Exception as e:
        logger.exception("Erro inesperado ao buscar transação por ID %s", id_transacao)
        st.error(f"Erro inesperado ao buscar transação por ID: {e}")

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
        id_transacao  # valor do WHERE (último %s)
    )

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Execução: Passa o SQL e a tupla de valores
        cursor.execute(sql_update, valores)
        conn.commit()
        consultar_dados.clear()
        return True

    except psycopg2.Error as ex:
        logger.exception("Erro de banco ao atualizar transação")
        st.error(f"Erro do banco de dados ao atualizar transação: {ex}")
        if conn: conn.rollback()
        return False

    except Exception as e:
        logger.exception("Erro inesperado ao atualizar transação")
        st.error(f"Erro inesperado ao atualizar transação: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

def atualizar_status_acerto(lista_ids):
    conn = None
    # Verifica se há IDs para evitar erro SQL e trabalho desnecessário
    if not lista_ids:
        return True 

    # Query usa UNNEST para desempacotar a lista de IDs do Python em valores SQL
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
        consultar_dados.clear()
        return True

    except psycopg2.Error as ex:
        logger.exception("Erro de banco ao realizar acerto múltiplo")
        st.error(f"Erro do banco de dados ao realizar acerto múltiplo: {ex}")
        if conn: conn.rollback()
        return False

    except Exception as e:
        logger.exception("Erro inesperado ao realizar acerto múltiplo")
        st.error(f"Erro inesperado ao realizar acerto múltiplo: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

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
        consultar_dados.clear()
        return True

    except psycopg2.Error as ex:
        logger.exception("Erro de banco ao deletar em %s", tabela_lower)
        st.error(f"Erro do banco de dados ao deletar em {tabela_lower}: {ex}")
        if conn: conn.rollback()
        return False

    except Exception as e:
        logger.exception("Erro inesperado ao deletar em %s", tabela_lower)
        st.error(f"Erro inesperado: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()

def deletar_transacoes(lista_ids):
    conn = None
    if not lista_ids:
        return True

    sql_delete = "DELETE FROM stg_transacoes WHERE id_transacao = ANY(%s);"

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_delete, (lista_ids,))
        conn.commit()
        consultar_dados.clear()
        return True

    except psycopg2.Error as ex:
        logger.exception("Erro de banco ao deletar transações")
        st.error(f"Erro do banco de dados ao deletar transações: {ex}")
        if conn: conn.rollback()
        return False

    except Exception as e:
        logger.exception("Erro inesperado ao deletar transações")
        st.error(f"Erro inesperado ao deletar transações: {e}")
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
        consultar_dados.clear()
        return True

    except psycopg2.Error as ex:
        logger.exception("Erro de banco ao atualizar em %s", tabela_lower)
        st.error(f"Erro do banco de dados ao atualizar em {tabela_lower}: {ex}")
        if conn: conn.rollback()
        return False

    except Exception as e:
        logger.exception("Erro inesperado ao atualizar em %s", tabela_lower)
        st.error(f"Erro inesperado: {e}")
        if conn: conn.rollback()
        return False

    finally:
        if conn: conn.close()
