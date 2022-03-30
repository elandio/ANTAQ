USE [LAKE_CLEAN]
GO

/****** Object:  Table [dbo].[ATRACACAO_FATO]    Script Date: 30/03/2022 17:42:24 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ATRACACAO_FATO](
	[IDATRACACAO] [int] NULL,
	[CDTUP] [varchar](50) NULL,
	[IDBERCO] [varchar](50) NULL,
	[BERCO] [varchar](50) NULL,
	[PORTO_ATRACACAO] [varchar](50) NULL,
	[APELIDO_INSTALACAO_PORTUARIA] [varchar](50) NULL,
	[COMPLEXO_PORTUARIO] [varchar](50) NULL,
	[TIPO_DA_AUTORIDADE_PORTUARIA] [varchar](50) NULL,
	[DATA_ATRACACAO] [datetime] NULL,
	[DATA_CHEGADA] [datetime] NULL,
	[DATA_DESATRACACAO] [datetime] NULL,
	[DATA_INICIO_OPERACAO] [datetime] NULL,
	[DATA_TERMINO_OPERACAO] [datetime] NULL,
	[ANO] [char](4) NULL,
	[MES] [char](3) NULL,
	[TIPO_DE_OPERACAO] [varchar](50) NULL,
	[TIPO_DE_NAVEGACAO_DA_ATRACACAO] [varchar](50) NULL,
	[NACIONALIDADE_DO_ARMADOR] [char](1) NULL,
	[FLAGMCOPERACAOATRACACAO] [char](1) NULL,
	[TERMINAL] [varchar](50) NULL,
	[MUNICIPIO] [varchar](50) NULL,
	[UF] [varchar](50) NULL,
	[SGUF] [char](2) NULL,
	[REGIAO_GEOGRAFICA] [varchar](50) NULL,
	[NUM_DA_CAPITANIA] [varchar](50) NULL,
	[NUM_DO_IMO] [varchar](50) NULL
) ON [PRIMARY]
GO

