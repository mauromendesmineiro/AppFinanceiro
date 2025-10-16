USE Financeiro
GO

CREATE TABLE dim_TipoTransacao (
    ID_TipoTransacao INT PRIMARY KEY IDENTITY(1,1), -- Chave Primária, Identidade (Auto-Incremento)
    DSC_TipoTransacao VARCHAR(100) NOT NULL UNIQUE, -- Descrição do Tipo de Transação
    DT_Criacao DATETIME DEFAULT GETDATE() -- Data de Criação do Registro (Bom para auditoria)
)

CREATE TABLE dim_Categoria (
    ID_Categoria INT PRIMARY KEY IDENTITY(1,1), -- Chave Primária, Identidade (Auto-Incremento)
    ID_TipoTransacao INT NOT NULL, -- Chave Estrangeira para vincular a Categoria ao Tipo de Transação (Receita/Despesa)
    DSC_CategoriaTransacao VARCHAR(100) NOT NULL UNIQUE, -- Descrição da Categoria (ex: Alimentação, Moradia, Salário)
    DT_Criacao DATETIME DEFAULT GETDATE(), -- Data de Criação do Registro
    
    -- CHAVE ESTRANGEIRA (Foreign Key)
    CONSTRAINT FK_Categoria_TipoTransacao
        FOREIGN KEY (ID_TipoTransacao)
        REFERENCES dim_TipoTransacao (ID_TipoTransacao)
)

CREATE TABLE dim_Subcategoria (
    ID_Subcategoria INT PRIMARY KEY IDENTITY(1,1), -- Chave Primária, Identidade (Auto-Incremento)
    ID_Categoria INT NOT NULL, -- Chave Estrangeira para vincular a Subcategoria à Categoria Pai
    DSC_SubcategoriaTransacao VARCHAR(100) NOT NULL, -- Descrição da Subcategoria (ex: Almoço, Supermercado, Aluguel)
    DT_Criacao DATETIME DEFAULT GETDATE(), -- Data de Criação do Registro
    
    -- CHAVE ESTRANGEIRA (Foreign Key)
    CONSTRAINT FK_Subcategoria_Categoria
        FOREIGN KEY (ID_Categoria)
        REFERENCES dim_Categoria (ID_Categoria),
        
    -- Garante que a combinação de Categoria e Subcategoria seja única
    CONSTRAINT UQ_Subcategoria_Por_Categoria
        UNIQUE (ID_Categoria, DSC_SubcategoriaTransacao)
)

CREATE TABLE dim_Usuario (
    ID_Usuario INT PRIMARY KEY IDENTITY(1,1), -- Chave Primária, Identidade (Auto-Incremento)
    DSC_Nome VARCHAR(255) NOT NULL, -- Nome completo do Usuário
    --DSC_Email VARCHAR(255) NOT NULL UNIQUE, -- Email do Usuário (será o login, deve ser único)
    --DSC_SenhaHash VARCHAR(255) NULL, -- Armazena o hash da senha (para segurança)
    --DT_Criacao DATETIME DEFAULT GETDATE(), -- Data de Criação do Registro
    --DT_UltimoAcesso DATETIME NULL -- Data do último acesso do usuário
)

CREATE TABLE stg_Transacoes (
    ID_Transacao INT PRIMARY KEY IDENTITY(1,1), -- Chave Primária, Identidade (Auto-Incremento)
    
    -- Campos de Data e Dimensões (Mantendo as convenções de prefixo e nome)
    DT_DataTransacao DATE NOT NULL,             -- Data em que a transação ocorreu
    
    -- Campos de CÓDIGO (Chaves) e DESCRIÇÃO (Valores) das Dimensões, como é comum em Staging
    ID_TipoTransacao INT NOT NULL,              -- Chave do Tipo de Transação (Receita/Despesa)
    DSC_TipoTransacao VARCHAR(100) NOT NULL,    -- Descrição do Tipo de Transação
    ID_Categoria INT NOT NULL,                  -- Chave da Categoria
    DSC_CategoriaTransacao VARCHAR(100) NOT NULL, -- Descrição da Categoria
    ID_Subcategoria INT NOT NULL,               -- Chave da Subcategoria
    DSC_SubcategoriaTransacao VARCHAR(100) NOT NULL, -- Descrição da Subcategoria
    ID_Usuario INT NOT NULL,                    -- NOVO: Chave do Usuário (Quem fez/registrou a transação)
    DSC_NomeUsuario VARCHAR(255) NOT NULL,      -- NOVO: Nome do Usuário
    
    -- Campos da Transação
    DSC_Transacao VARCHAR(100) NOT NULL,        -- Descrição detalhada da transação
    VL_Transacao DECIMAL(10, 2) NOT NULL,       -- Valor da Transação
    
    -- Campos de controle (Ajustados para seguir o prefixo CD_)
    CD_QuemPagou VARCHAR(50) NOT NULL,          -- Quem pagou a transação (Nome ou ID)
    CD_EDividido CHAR(1) NOT NULL,              -- Flag: A transação foi dividida? ('S' ou 'N')
    CD_FoiDividido CHAR(1) NOT NULL,            -- Flag: A transação faz parte de uma divisão? ('S' ou 'N')
    DT_CriacaoRegistro DATETIME DEFAULT GETDATE() -- Data/Hora de quando o registro foi inserido no Staging

    -- CHAVES ESTRANGEIRAS
    CONSTRAINT FK_stg_TipoTransacao
        FOREIGN KEY (ID_TipoTransacao)
        REFERENCES dim_TipoTransacao (ID_TipoTransacao),
        
    CONSTRAINT FK_stg_Categoria
        FOREIGN KEY (ID_Categoria)
        REFERENCES dim_Categoria (ID_Categoria),
        
    CONSTRAINT FK_stg_Subcategoria
        FOREIGN KEY (ID_Subcategoria)
        REFERENCES dim_Subcategoria (ID_Subcategoria),
        
    CONSTRAINT FK_stg_Usuario
        FOREIGN KEY (ID_Usuario)
        REFERENCES dim_Usuario (ID_Usuario)
);

CREATE TABLE fact_Salario (
    ID_Salario INT PRIMARY KEY IDENTITY(1,1), -- Chave Primária, Identidade (Auto-Incremento)
    
    -- Chaves Estrangeiras (Vínculos com as Dimensões)
    ID_Usuario INT NOT NULL,                  -- Quem recebeu o salário
    
    -- Fato e Medidas (Valores)
    VL_Salario DECIMAL(10, 2) NOT NULL,       -- Valor líquido do salário
    
    -- Tempo e Descrição
    DT_Recebimento DATE NOT NULL,             -- Data efetiva do recebimento do salário
    DSC_Observacao VARCHAR(255) NULL,         -- Observações, ex: "Salário Janeiro/2025"
    
    -- Data de Criação do Registro
    DT_CriacaoRegistro DATETIME DEFAULT GETDATE(), -- Data/Hora de quando o registro foi inserido no sistema
    
    -- CHAVE ESTRANGEIRA
    CONSTRAINT FK_Salario_Usuario
        FOREIGN KEY (ID_Usuario)
        REFERENCES dim_Usuario (ID_Usuario)
);
GO

CREATE OR ALTER VIEW vw_dim_TipoTransacao AS
SELECT ID_TipoTransacao AS ID,                     -- Chave Primária simplificada
    DSC_TipoTransacao AS 'Tipo de Transacao',         -- Nome amigável para exibição
    DT_Criacao AS DataCriacao                   -- Alias para data de registro
FROM dim_TipoTransacao;
GO

CREATE OR ALTER VIEW vw_dim_Categoria AS
SELECT C.ID_Categoria AS ID,                       -- Chave Primária simplificada
    T.DSC_TipoTransacao AS 'Tipo de Transacao',       -- Exibe o nome do Tipo (JOIN)
    C.DSC_CategoriaTransacao AS Categoria,      -- Nome amigável para exibição
    C.DT_Criacao AS DataCriacao
FROM dim_Categoria AS C
INNER JOIN dim_TipoTransacao AS T
    ON C.ID_TipoTransacao = T.ID_TipoTransacao; -- Junta com o Tipo para mostrar o nome
GO

CREATE OR ALTER VIEW vw_dim_Subcategoria AS
SELECT S.ID_Subcategoria AS ID,                    -- Chave Primária simplificada
    C.DSC_CategoriaTransacao AS Categoria,      -- Exibe o nome da Categoria Pai (JOIN)
    S.DSC_SubcategoriaTransacao AS Subcategoria, -- Nome amigável para exibição
    S.DT_Criacao AS DataCriacao
FROM dim_Subcategoria AS S
INNER JOIN dim_Categoria AS C
    ON S.ID_Categoria = C.ID_Categoria;         -- Junta com a Categoria para mostrar o nome
GO

CREATE OR ALTER VIEW vw_dim_Usuario AS
SELECT ID_Usuario AS ID,                       -- Chave Primária simplificada
    DSC_Nome AS Nome                        -- Nome amigável para exibição
FROM dim_Usuario;
GO

CREATE OR ALTER VIEW vw_fact_Salario AS
SELECT
    S.ID_Salario AS ID,                         -- Chave Primária simplificada
    U.DSC_Nome AS Usuario,                      -- Exibe o nome do Usuário (JOIN)
    S.VL_Salario AS Valor,                      -- Valor do Salário
    S.DT_Recebimento AS 'Data do Recebimento',        -- Data de recebimento
    S.DSC_Observacao AS Observacao,             -- Observações
    S.DT_CriacaoRegistro AS 'Data do Registro'        -- Data de criação do registro
FROM fact_Salario AS S
INNER JOIN dim_Usuario AS U
    ON S.ID_Usuario = U.ID_Usuario;             -- Junta com o Usuário para mostrar o nome
GO

CREATE OR ALTER VIEW vw_Rateio AS
SELECT
    -- Agrupadores de Tempo
    YEAR(S.DT_Recebimento) AS Ano,                  -- Ano do recebimento (Contexto de rateio)
    MONTH(S.DT_Recebimento) AS Mes,                 -- Mês do recebimento (Contexto de rateio)
    
    -- Informações do Usuário
    S.ID_Usuario,                                   -- Chave do Usuário
    U.DSC_Nome AS NomeUsuario,                      -- Nome do Usuário
    
    -- Valores de Salário (apenas para contexto)
    S.VL_Salario,                                   -- Salário individual do registro
  
    -- Soma Total dos Salários no Mes/Ano
    SUM(S.VL_Salario) OVER (
        PARTITION BY YEAR(S.DT_Recebimento), MONTH(S.DT_Recebimento)
    ) AS VL_TotalSalarioMes,
    
    -- Proporção de Rateio como PERCENTUAL (arredondado para 2 casas decimais)
    ROUND(
        (CAST(S.VL_Salario AS DECIMAL(18, 4)) / SUM(S.VL_Salario) OVER (
            PARTITION BY YEAR(S.DT_Recebimento), MONTH(S.DT_Recebimento)
        )), 4
    ) AS VL_Rateio
    
FROM fact_Salario AS S
INNER JOIN dim_Usuario AS U
    ON S.ID_Usuario = U.ID_Usuario;
GO

CREATE OR ALTER VIEW vw_AcertoDetalhe AS
SELECT T.DT_DataTransacao,
    T.DSC_TipoTransacao,
    T.DSC_CategoriaTransacao,
    T.DSC_SubcategoriaTransacao,
    T.DSC_Transacao,
    T.VL_Transacao AS VL_TotalTransacao,
    T.CD_QuemPagou,
    R.NomeUsuario AS CD_QuemDeve,
    ROUND((T.VL_Transacao * R.VL_Rateio), 2) AS VL_Proporcional,
    ROUND(
        (CASE WHEN T.CD_QuemPagou = R.NomeUsuario THEN T.VL_Transacao ELSE 0.00 END) 
        - 
        (T.VL_Transacao * R.VL_Rateio)
    , 2) AS VL_AcertoTransacao
FROM stg_Transacoes AS T
INNER JOIN vw_Rateio AS R
    ON YEAR(T.DT_DataTransacao) = R.Ano
    AND MONTH(T.DT_DataTransacao) = R.Mes
WHERE T.CD_EDividido = 'S'
AND T.CD_FoiDividido = 'N'
AND R.NomeUsuario <> T.CD_QuemPagou
GO

CREATE OR ALTER VIEW vw_AcertoMensal AS
    SELECT YEAR(DT_DataTransacao) AS Ano,
        MONTH(DT_DataTransacao) AS Mes,
        CD_QuemDeve,
        SUM(VL_AcertoTransacao) AS VL_SaldoAcertoMensal
    FROM vw_AcertoDetalhe
    GROUP BY YEAR(DT_DataTransacao), MONTH(DT_DataTransacao), CD_QuemDeve;
    GO

CREATE OR ALTER VIEW vw_AcertoTotal AS
SELECT CD_QuemDeve AS NomeUsuario,
    SUM(VL_SaldoAcertoMensal) AS VL_SaldoTotal
FROM vw_AcertoMensal
GROUP BY CD_QuemDeve;
GO

CREATE OR ALTER VIEW vw_AcertoMensal AS
SELECT YEAR(T.DT_DataTransacao) AS Ano,
    MONTH(T.DT_DataTransacao) AS Mes,
    R.NomeUsuario AS CD_QuemDeve,
    SUM(
        (CASE WHEN T.CD_QuemPagou = R.NomeUsuario THEN T.VL_Transacao ELSE 0.00 END)
        -
        (T.VL_Transacao * R.VL_Rateio)
    ) AS VL_SaldoAcertoMensal
FROM stg_Transacoes AS T
INNER JOIN vw_Rateio AS R 
    ON YEAR(T.DT_DataTransacao) = R.Ano
    AND MONTH(T.DT_DataTransacao) = R.Mes
WHERE T.CD_EDividido = 'S' AND T.CD_FoiDividido = 'N'
GROUP BY YEAR(T.DT_DataTransacao), MONTH(T.DT_DataTransacao), R.NomeUsuario;
GO

CREATE OR ALTER VIEW vw_AcertoTotal
AS
SELECT R.NomeUsuario AS NomeUsuario,
    SUM(
        (CASE WHEN T.CD_QuemPagou = R.NomeUsuario THEN T.VL_Transacao ELSE 0.00 END)
        -
        (T.VL_Transacao * R.VL_Rateio)
    ) AS VL_SaldoTotal
FROM stg_Transacoes AS T
INNER JOIN vw_Rateio AS R 
    ON YEAR(T.DT_DataTransacao) = R.Ano 
    AND MONTH(T.DT_DataTransacao) = R.Mes
WHERE T.CD_EDividido = 'S' AND T.CD_FoiDividido = 'N'
GROUP BY R.NomeUsuario;
GO
