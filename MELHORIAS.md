# Avaliação do Projeto e Plano de Melhorias — AppFinanceiro

Documento gerado a partir da avaliação completa do arquivo `sqlserver/main.py`
(aplicação Streamlit + PostgreSQL/Neon para controle de gastos do casal).

O objetivo é registrar o estado atual, os problemas encontrados e um plano de
evolução. Os itens marcados com ✅ já foram corrigidos no branch
`melhorias/refatoracao-e-correcoes`.

---

## 1. Visão geral

A aplicação é um app financeiro em **Streamlit** com modelo dimensional
(`dim_*`, `fact_*`, `stg_transacoes`) sobre PostgreSQL (Neon). Funciona, mas todo
o código vive em **um único arquivo de ~2.300 linhas** com bastante duplicação,
tratamento de erros inconsistente e algumas funções com assinaturas quebradas.

Pontos fortes:
- Modelagem de dados clara (dimensões + staging + views `vw_*`).
- Uso de queries parametrizadas (`%s`) na maioria dos pontos de escrita.
- Dashboard rico (passado/futuro, categorias/subcategorias, projeção).

---

## 2. Problemas críticos (bugs reais)

### 2.1 ✅ Assinatura incompatível em `deletar_registro_dimensao`
A função era definida como `deletar_registro_dimensao(tabela, id_registro)`, mas
é **sempre chamada com 3 argumentos**: `("dim_categoria", "id_categoria", id_del)`.
Resultado: `TypeError` ao tentar excluir qualquer Tipo/Categoria/Subcategoria.
**Correção:** assinatura ajustada para `(tabela, id_coluna, id_registro)`, usando
o `id_coluna` informado em vez de deduzir pelo nome da tabela.

### 2.2 ✅ Assinatura incompatível em `atualizar_registro_dimensao`
Definida como `(tabela, campos, valores, id_registro)`, mas chamada como
`("dim_tipotransacao", "id_tipotransacao", id_selecionado, campos_valores)` —
onde `campos_valores` é um **dicionário** `{coluna: valor}`. A ordem e os tipos
não batiam, quebrando toda edição de cadastros.
**Correção:** assinatura ajustada para `(tabela, id_coluna, id_registro, campos_valores)`,
montando o `SET` a partir das chaves/valores do dicionário.

### 2.3 ✅ `where_clause` inexistente em `consultar_dados`
`acerto_multiplo_transacoes()` chamava `consultar_dados(..., where_clause=...)`,
parâmetro que **não existe**, sempre caindo num `except:` "pelado". A lógica era
frágil e mascarava erros.
**Correção:** removida a chamada inválida; o filtro `cd_foidividido = 'N'` é feito
em Pandas de forma explícita.

---

## 3. Segurança (prioridade alta)

### 3.1 ✅ Senhas em texto plano
`autenticar_usuario` e `login_page` comparavam `senha = %s` diretamente, com as
senhas **armazenadas sem hash** em `dim_usuario`.
**Correção:** implementado hashing **bcrypt** com helpers `gerar_hash_senha`,
`verificar_senha` e `_migrar_senha_para_hash`. A `autenticar_usuario` agora valida
a senha em Python (suporta hash) e faz **migração automática e suave**: no primeiro
login bem-sucedido com senha legada (texto plano), o valor é regravado como hash.
Nenhum login atual quebra.
> ⚠️ Ação no banco: garantir que `dim_usuario.senha` comporte ≥ 60 caracteres
> (`VARCHAR(255)` ou `TEXT`). Não há troca de SGBD nem script de migração manual.

### 3.2 Credenciais e SSL
Conexão usa `st.secrets` (correto) e `sslmode='require'` (correto). Manter o
`secrets.toml` fora do versionamento (já coberto pelo `.gitignore`).

### 3.3 ✅ Código duplicado de autenticação
`autenticar_usuario()` existia mas **não era usada** — `login_page()` refazia a
query inline com comparação em texto plano.
**Correção:** `login_page()` agora delega para `autenticar_usuario()`, eliminando
a duplicação e o caminho inseguro.

---

## 4. Qualidade de código e manutenção

| # | Item | Situação |
|---|------|----------|
| 4.1 | Arquivo único de ~2.300 linhas | Sugerido dividir em `db.py`, `forms.py`, `dashboard.py`, `auth.py` |
| 4.2 | ✅ `formatar_moeda` e funções de cor (`color_saldo`, etc.) duplicadas em vários pontos | Consolidadas nos helpers globais `formatar_moeda`, `_para_float_br` e `cor_saldo` |
| 4.3 | ✅ `except (psycopg2.Error, TypeError, Exception)` e `except Exception:` silencioso | Tuplas redundantes separadas em `psycopg2.Error` + fallback; logging (`logger.exception`) adicionado em todos os handlers |
| 4.4 | ✅ `id_tipo = 1 if novo_tipo == 'Despesas' else 2` (IDs mágicos) | Substituído por lookup real via `tipos_map` de `dim_tipotransacao`; lista de tipos do form de edição também vem do banco |
| 4.5 | ✅ `pd.read_sql` gera `UserWarning` (recomenda SQLAlchemy) | Reads passam o engine SQLAlchemy (`pd.read_sql(text(...), engine)`); `buscar_transacao_por_id` usa parâmetro nomeado |
| 4.6 | ✅ `get_connection` abre conexão nova a cada query; `@st.cache_resource` está comentado | Engine SQLAlchemy com pool cacheado (`@st.cache_resource`, `pool_pre_ping`, `pool_recycle`); `get_connection()` devolve conexão do pool |
| 4.7 | ✅ Comentários extensos de debug/histórico ("CORREÇÃO", "LINHA 82", 💡) | Ruído removido |
| 4.8 | ✅ `requirements.txt` sem versões fixas | Versões pinadas |

---

## 5. UX e funcionalidades futuras

- Filtros de período no Dashboard (hoje fixos em 13 meses passados / 12 futuros).
- Exportar dados (CSV/Excel) das transações e do acerto.
- Confirmação visual padronizada para exclusões (hoje cada formulário reimplementa).
- Indicadores-resumo (KPIs) no topo do Dashboard.

---

## 6. O que foi alterado neste branch

1. ✅ Correção das assinaturas de `deletar_registro_dimensao` e
   `atualizar_registro_dimensao` (bugs que quebravam edição/exclusão).
2. ✅ Remoção da chamada inválida `where_clause` em `acerto_multiplo_transacoes`
   e do `except:` pelado.
3. ✅ Pinagem de versões mínimas em `requirements.txt`.
4. ✅ Este documento `MELHORIAS.md`.
5. ✅ Hashing de senha com bcrypt e migração automática (itens 3.1 e 3.3).
6. ✅ Consolidação das funções de formatação/cor em helpers globais
   (`formatar_moeda`, `_para_float_br`, `cor_saldo`) — item 4.2.
7. ✅ Limpeza de comentários de debug/histórico ("CORREÇÃO", "LINHA 82",
   "MUDANÇA AQUI", 💡, etc.) — item 4.7.
8. ✅ Eliminação dos IDs mágicos de tipo de transação (`1 if ... else 2`),
   agora resolvidos via `dim_tipotransacao` — item 4.4.
9. ✅ Tratamento de exceções: tuplas redundantes separadas em `psycopg2.Error`
   + fallback e introdução de `logging` em todos os handlers — item 4.3.
10. ✅ Engine SQLAlchemy com pool de conexões (`get_engine` cacheado) — os
    reads usam o engine, eliminando o `UserWarning` do pandas, e
    `get_connection()` passa a devolver conexões do pool — itens 4.5 e 4.6.

> ⚠️ Nova dependência: `SQLAlchemy>=2.0` (adicionada ao `requirements.txt`).

A refatoração estrutural em módulos (item 4.1) fica documentada como próximo passo
por exigir mudanças de arquitetura mais amplas, sem caráter crítico.
