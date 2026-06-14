"""Entrypoint Streamlit: configuração da página, sessão e navegação."""
import streamlit as st
st.set_page_config(
    layout="wide",  # Define a largura máxima como a largura do navegador
    initial_sidebar_state="auto"
)
from auth import login_page
from forms import formulario_categoria, formulario_salario, formulario_subcategoria, formulario_tipo_transacao, formulario_transacao, formulario_usuario, pagina_acerto_controle
from dashboard import dashboard
if 'menu_selecionado' not in st.session_state:
    st.session_state.menu_selecionado = "Dashboard"

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
        # Usar dsc_nome (nome_completo) em vez de login
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

        if st.button("💰 Acerto & Correção", key="btn_acerto", use_container_width=True):
            st.session_state.menu_selecionado = "Acerto de Contas"

        st.subheader("Cadastros (Dimensões)")

        # Dicionário de Opções
        opcoes_cadastro = {
            "💳 Tipos de Transação": "Tipos de Transação", 
            "🏷️ Categorias": "Categorias",        
            "📝 Subcategorias": "Subcategorias",     
            "👥 Usuários": "Usuários",          
            "💰 Salário": "Salário",
        }

        # Layout de 2 botões por linha para Cadastros
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

        # Estilo CSS específico para o botão "🛑 Sair"
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

if __name__ == "__main__":
    main()
