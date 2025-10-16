-- CREATE DATABASE Financeiro
-- GO

USE Financeiro
GO

-- DROP TABLE IF EXISTS stg_Transacoes
--     CREATE TABLE stg_Transacoes (
--         ID INT PRIMARY KEY IDENTITY(1,1), -- Chave primária e auto-incremento
--         DT_DataTransacao DATE NOT NULL,
--         ID_Tipo INT NOT NULL,
--         DSC_TipoTransacao VARCHAR(12) NOT NULL, -- Receita, Despesa e Investimento
--         ID_Categoria INT NOT NULL,
--         DSC_CategoriaTransacao VARCHAR(50) NOT NULL,
--         ID_Subcategoria INT NOT NULL,
--         DSC_SubcategoriaTransacao VARCHAR(50) NOT NULL,
--         DSC_Transacao VARCHAR(100) NOT NULL,
--         VL_Transacao DECIMAL(10, 2) NOT NULL,
--         NM_QuemPagou VARCHAR(5) NOT NULL, -- Mauro ou Marta
--         CD_EDividido CHAR(1) NOT NULL, -- 'S' para Sim, 'n' para Não
--         CD_FoiDividido CHAR(1) NOT NULL -- 'S' para Sim, 'n' para Não
--     );

-- DROP TABLE IF EXISTS dim_TipoTransacao
--     CREATE TABLE dim_TipoTransacao (
--         ID_Tipo INT PRIMARY KEY IDENTITY(1,1),
--         DSC_TipoTransacao VARCHAR(12) NOT NULL UNIQUE
--     );

-- DROP TABLE IF EXISTS dim_Categoria
--     CREATE TABLE dim_Categoria (
--         ID_Categoria INT PRIMARY KEY IDENTITY(1,1),
--         DSC_CategoriaTransacao VARCHAR(50) NOT NULL UNIQUE
--     );

-- DROP TABLE IF EXISTS dim_Subcategoria
--     CREATE TABLE dim_Subcategoria (
--         ID_Subcategoria INT PRIMARY KEY IDENTITY(1,1),
--         ID_Categoria INT NOT NULL,
--         DSC_SubcategoriaTransacao VARCHAR(50) NOT NULL,
--         FOREIGN KEY (ID_Categoria) REFERENCES Dim_Categoria(ID_Categoria)
--     );

SELECT * FROM dim_Categoria

GO