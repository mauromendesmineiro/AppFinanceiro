"""Autenticação: hashing bcrypt, login e migração de senha."""
import bcrypt
import streamlit as st
from helpers import logger
from db import get_connection

def gerar_hash_senha(senha):
    """Gera um hash bcrypt (string) a partir de uma senha em texto plano."""
    hash_bytes = bcrypt.hashpw(senha.encode("utf-8"), bcrypt.gensalt())
    return hash_bytes.decode("utf-8")

def _eh_hash_bcrypt(valor):
    """Indica se o valor armazenado já é um hash bcrypt (prefixos $2a/$2b/$2y)."""
    return isinstance(valor, str) and valor.startswith(("$2a$", "$2b$", "$2y$"))

def verificar_senha(senha_digitada, senha_armazenada):
    """
    Verifica a senha digitada contra o valor armazenado.

    Retorna uma tupla (valida, precisa_migrar):
      - valida: True se a senha confere.
      - precisa_migrar: True quando o valor armazenado ainda está em texto
        plano (legado) e deve ser regravado como hash bcrypt.
    """
    if senha_armazenada is None:
        return False, False

    if _eh_hash_bcrypt(senha_armazenada):
        try:
            valida = bcrypt.checkpw(
                senha_digitada.encode("utf-8"),
                senha_armazenada.encode("utf-8"),
            )
        except ValueError:
            valida = False
        return valida, False

    # Legado: senha em texto plano. Se conferir, sinaliza migração para hash.
    valida = senha_digitada == senha_armazenada
    return valida, valida

def _migrar_senha_para_hash(conn, id_usuario, senha_digitada):
    """Regrava a senha do usuário como hash bcrypt (migração automática)."""
    try:
        novo_hash = gerar_hash_senha(senha_digitada)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE dim_usuario SET senha = %s WHERE id_usuario = %s;",
            (novo_hash, id_usuario),
        )
        conn.commit()
    except Exception as e:
        logger.warning("Falha ao migrar senha para hash: %s", e)
        # Falha na migração não deve impedir o login; apenas registra.
        conn.rollback()
        print(f"Aviso: não foi possível migrar a senha para hash: {e}")

def autenticar_usuario(login, senha):
    """
    Verifica login e senha contra dim_usuario, com suporte a senhas em hash
    bcrypt e migração automática de senhas legadas (texto plano).
    Retorna {id_usuario, nome_completo, login} em caso de sucesso, ou {}.
    """
    conn = None
    usuario_info = {}
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Busca o usuário pelo login e valida a senha em Python (suporta hash).
        cursor.execute(
            "SELECT id_usuario, dsc_nome, login, senha FROM dim_usuario WHERE login = %s;",
            (login,),
        )
        resultado = cursor.fetchone()

        if resultado:
            id_usuario, nome_completo, login_db, senha_armazenada = resultado
            valida, precisa_migrar = verificar_senha(senha, senha_armazenada)

            if valida:
                if precisa_migrar:
                    _migrar_senha_para_hash(conn, id_usuario, senha)

                usuario_info = {
                    "id_usuario": id_usuario,
                    "nome_completo": nome_completo,
                    "login": login_db,
                }

    except Exception as e:
        logger.exception("Erro na autenticação de usuário")
        st.error("Ocorreu um erro na autenticação. Verifique a conexão com o banco de dados e as credenciais.")
        print(f"Erro de autenticação: {e}")
        usuario_info = {}
    finally:
        if conn:
            conn.close()

    return usuario_info

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
                # Autenticação centralizada (suporta hash bcrypt e migração automática)
                usuario_info = autenticar_usuario(login_input, senha_input)

                if usuario_info:
                    st.session_state.logged_in = True
                    st.session_state.id_usuario_logado = usuario_info["id_usuario"]
                    st.session_state.login = usuario_info["login"]
                    st.session_state.nome_completo = usuario_info["nome_completo"]
                    st.session_state.menu_selecionado = "Dashboard"
                    st.success(f"Bem-vindo, {usuario_info['nome_completo']}! Acesso concedido.")
                    st.rerun()
                else:
                    st.error("Login ou senha incorretos. Tente novamente.")
