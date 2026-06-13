# AppFinanceiro

Aplicação **Streamlit + PostgreSQL/Neon** para controle de gastos do casal
(modelo dimensional `dim_*` / `fact_*` / `stg_transacoes`).

O código da aplicação fica em [`sqlserver/main.py`](sqlserver/main.py).

## Requisitos

- [uv](https://docs.astral.sh/uv/) (gerenciador de ambiente/dependências)
- Python `>=3.11` (o uv instala automaticamente se necessário)

## Ambiente e dependências

As dependências diretas são declaradas em `pyproject.toml` e travadas em
`uv.lock` (versões reproduzíveis). O `sqlserver/requirements.txt` é **gerado**
a partir do lock (usado no deploy do Streamlit Cloud) — não edite à mão.

```bash
# Cria o .venv e instala exatamente as versões do uv.lock
uv sync
```

Comandos úteis:

```bash
uv add <pacote>           # adiciona dependência (atualiza pyproject + uv.lock)
uv lock --upgrade         # atualiza o lock respeitando as restrições
# Regenera o requirements.txt derivado após mudar dependências:
uv export --no-hashes --no-emit-project --no-dev -o sqlserver/requirements.txt
```

## Credenciais (secrets)

A conexão usa `st.secrets["postgresql"]`. Crie o arquivo
`.streamlit/secrets.toml` (já ignorado pelo Git):

```toml
[postgresql]
server   = "<host-do-neon>"
database = "<database>"
username = "<usuario>"
password = "<senha>"
port     = "5432"
```

## Executar

```bash
uv run streamlit run sqlserver/main.py
```

> A senha em `dim_usuario.senha` é armazenada com **bcrypt**. No primeiro login
> com senha legada (texto plano) ela é migrada automaticamente para hash, então
> a coluna precisa comportar ≥ 60 caracteres (`VARCHAR(255)` ou `TEXT`).
