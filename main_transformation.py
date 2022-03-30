from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
import findspark
import pandas as pd
import pyodbc
import os
os.environ['PYSPARK_PYTHON'] = 'python'
findspark.init()


spark = SparkSession.builder\
    .master('local')\
    .appName('Pandas')\
    .getOrCreate()


def conexao_db(banco):
    host = '127.0.0.1'
    login = 'py'
    pwd = '123456'

    try:
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                              host + ';DATABASE=' + banco + ';UID=' + login + ';PWD=' + pwd)
    except pyodbc.InterfaceError as e:
        return f'Erro de conexão - {e}'
    else:
        return cnxn


def carrega_carga_fato():
    cnxn_origem = conexao_db('LAKE_RAW')
    cnxn_destino = conexao_db('LAKE_CLEAN')
    cursor_destino = cnxn_destino.cursor()

    query = f'select * from TBL_CARGA'
    pdf = pd.read_sql(query, cnxn_origem)
    df_origem = spark.createDataFrame(pdf)

    df_destino = df_origem.withColumn('IDCARGA', df_origem.IDCARGA.cast(IntegerType()))             \
        .withColumn('IDATRACACAO', df_origem.IDATRACACAO.cast(IntegerType()))   \
        .withColumn('TEU', df_origem.TEU.cast(IntegerType()))                   \
        .withColumn('QTCARGA', df_origem.QTCARGA.cast(IntegerType()))           \
        .withColumn('VLPESOCARGABRUTA', df_origem.VLPESOCARGABRUTA.cast(FloatType()))

    df_save = df_destino.toPandas()
    df_save.fillna('', inplace=True)
    df_save.reset_index(drop=True, inplace=True)

    for indice, linha in df_save.iterrows():
        cursor_destino.execute(
            'INSERT INTO [dbo].[CARGA_FATO](             ' +
            ' [IDCARGA]                                 ' +
            ',[IDATRACACAO]                             ' +
            ',[ORIGEM]                                  ' +
            ',[DESTINO]                                 ' +
            ',[CDMERCADORIA]                            ' +
            ',[TIPO_OPERACAO_DA_CARGA]                  ' +
            ',[CARGA_GERAL_ACONDICIONAMENTO]            ' +
            ',[CONTEINERESTADO]                         ' +
            ',[TIPO_NAVEGACAO]                          ' +
            ',[FLAGAUTORIZACAO]                         ' +
            ',[FLAGCABOTAGEM]                           ' +
            ',[FLAGCABOTAGEMMOVIMENTACAO]               ' +
            ',[FLAGCONTEINERTAMANHO]                    ' +
            ',[FLAGLONGOCURSO]                          ' +
            ',[FLAGMCOPERACAOCARGA]                     ' +
            ',[FLAGOFFSHORE]                            ' +
            ',[FLAGTRANSPORTEVIAINTERIOIR]              ' +
            ',[PERCURSO_TRANSPORTE_EM_VIAS_INTERIORES]  ' +
            ',[PERCURSO_TRANSPORTE_INTERIORES]          ' +
            ',[STNATUREZACARGA]                         ' +
            ',[STSH2]                                   ' +
            ',[STSH4]                                   ' +
            ',[NATUREZA_DA_CARGA]                       ' +
            ',[SENTIDO]                                 ' +
            ',[TEU]                                     ' +
            ',[QTCARGA]                                 ' +
            ',[VLPESOCARGABRUTA])                       ' +
            ' VALUES                                     ' +
            '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            linha['IDCARGA'],
            linha['IDATRACACAO'],
            linha['ORIGEM'],
            linha['DESTINO'],
            linha['CDMERCADORIA'],
            linha['TIPO_OPERACAO_DA_CARGA'],
            linha['CARGA_GERAL_ACONDICIONAMENTO'],
            linha['CONTEINERESTADO'],
            linha['TIPO_NAVEGACAO'],
            linha['FLAGAUTORIZACAO'],
            linha['FLAGCABOTAGEM'],
            linha['FLAGCABOTAGEMMOVIMENTACAO'],
            linha['FLAGCONTEINERTAMANHO'],
            linha['FLAGLONGOCURSO'],
            linha['FLAGMCOPERACAOCARGA'],
            linha['FLAGOFFSHORE'],
            linha['FLAGTRANSPORTEVIAINTERIOIR'],
            linha['PERCURSO_TRANSPORTE_EM_VIAS_INTERIORES'],
            linha['PERCURSO_TRANSPORTE_INTERIORES'],
            linha['STNATUREZACARGA'],
            linha['STSH2'],
            linha['STSH4'],
            linha['NATUREZA_DA_CARGA'],
            linha['SENTIDO'],
            linha['TEU'],
            linha['QTCARGA'],
            linha['VLPESOCARGABRUTA'])
        cnxn_destino.commit()

    cursor_destino.close()
    cnxn_origem.close()
    cnxn_destino.close()


def carrega_atracacao_fato():
    cnxn_origem = conexao_db('LAKE_RAW')
    cnxn_destino = conexao_db('LAKE_CLEAN')
    cursor_destino = cnxn_destino.cursor()

    query = f'select * from TBL_ATRACACAO'
    pdf = pd.read_sql(query, cnxn_origem)
    df_origem = spark.createDataFrame(pdf)
    df_destino = df_origem  \
        .withColumn('IDATRACACAO', df_origem.IDATRACACAO.cast(IntegerType()))\
        .withColumn('DATA_ATRACACAO', to_timestamp('DATA_ATRACACAO', 'MM/dd/yyyy HH:mm:ss'))\
        .withColumn('DATA_CHEGADA', to_timestamp('DATA_CHEGADA', 'MM/dd/yyyy HH:mm:ss'))\
        .withColumn('DATA_DESATRACACAO', to_timestamp('DATA_DESATRACACAO', 'MM/dd/yyyy HH:mm:ss'))\
        .withColumn('DATA_INICIO_OPERACAO', to_timestamp('DATA_INICIO_OPERACAO', 'MM/dd/yyyy HH:mm:ss'))\
        .withColumn('DATA_TERMINO_OPERACAO', to_timestamp('DATA_TERMINO_OPERACAO', 'MM/dd/yyyy HH:mm:ss'))

    # df_destino.show(2)
    df_save = df_destino.toPandas()
    df_save.fillna('', inplace=True)
    df_save.reset_index(drop=True, inplace=True)

   # df_save.show(2)

    for indice, linha in df_save.iterrows():
        cursor_destino.execute(
            'INSERT INTO [dbo].[ATRACACAO_FATO](     ' +
            '[IDATRACACAO]                          ' +
            ',[CDTUP]                               ' +
            ',[IDBERCO]                             ' +
            ',[BERCO]                               ' +
            ',[PORTO_ATRACACAO]                     ' +
            ',[APELIDO_INSTALACAO_PORTUARIA]        ' +
            ',[COMPLEXO_PORTUARIO]                  ' +
            ',[TIPO_DA_AUTORIDADE_PORTUARIA]        ' +
            ',[DATA_ATRACACAO]                      ' +
            ',[DATA_CHEGADA]                        ' +
            ',[DATA_DESATRACACAO]                   ' +
            ',[DATA_INICIO_OPERACAO]                ' +
            ',[DATA_TERMINO_OPERACAO]               ' +
            ',[ANO]                                 ' +
            ',[MES]                                 ' +
            ',[TIPO_DE_OPERACAO]                    ' +
            ',[TIPO_DE_NAVEGACAO_DA_ATRACACAO]      ' +
            ',[NACIONALIDADE_DO_ARMADOR]            ' +
            ',[FLAGMCOPERACAOATRACACAO]             ' +
            ',[TERMINAL]                            ' +
            ',[MUNICIPIO]                           ' +
            ',[UF]                                  ' +
            ',[SGUF]                                ' +
            ',[REGIAO_GEOGRAFICA]                   ' +
            ',[NUM_DA_CAPITANIA]                    ' +
            ',[NUM_DO_IMO]                          ' +
            ') VALUES                               ' +
            '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            linha['IDATRACACAO'],
            linha['CDTUP'],
            linha['IDBERCO'],
            linha['BERCO'],
            linha['PORTO_ATRACACAO'],
            linha['APELIDO_INSTALACAO_PORTUARIA'],
            linha['COMPLEXO_PORTUARIO'],
            linha['TIPO_DA_AUTORIDADE_PORTUARIA'],
            linha['DATA_ATRACACAO'],
            linha['DATA_CHEGADA'],
            linha['DATA_DESATRACACAO'],
            linha['DATA_INICIO_OPERACAO'],
            linha['DATA_TERMINO_OPERACAO'],
            linha['ANO'],
            linha['MES'],
            linha['TIPO_DE_OPERACAO'],
            linha['TIPO_DE_NAVEGACAO_DA_ATRACACAO'],
            linha['NACIONALIDADE_DO_ARMADOR'],
            linha['FLAGMCOPERACAOATRACACAO'],
            linha['TERMINAL'],
            linha['MUNICIPIO'],
            linha['UF'],
            linha['SGUF'],
            linha['REGIAO_GEOGRAFICA'],
            linha['NUM_DA_CAPITANIA'],
            linha['NUM_DO_IMO']
        )
        cnxn_destino.commit()

    cursor_destino.close()
    cnxn_origem.close()
    cnxn_destino.close()


carrega_carga_fato()
carrega_atracacao_fato()
spark.stop()
