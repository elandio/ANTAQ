import pandas as pd
import glob
import os
import re
from datetime import datetime, date, timedelta
import pyodbc


def importa_arq(arquivo):
    dados = pd.read_csv(
        'C:\\PROJETO\\ARQUIVOS\\' + arquivo, sep=';', low_memory=False)
    dados.fillna('', inplace=True)
    dados.reset_index(drop=True, inplace=True)
    return dados


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


def importa_tbl_carga():
    con = conexao_db('LAKE_RAW')
    cursor = con.cursor()
    dados = importa_arq('Carga.txt')

    for indice, linha in dados.iterrows():
        cursor.execute(
            'INSERT INTO [dbo].[TBL_CARGA](             ' +
            '[IDCARGA]                                  ' +
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
            ',[VLPESOCARGABRUTA]                       ' +
            ') VALUES                                    ' +
            '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            linha['IDCarga'],
            linha['IDAtracacao'],
            linha['Origem'],
            linha['Destino'],
            linha['CDMercadoria'],
            linha['Tipo Operação da Carga'],
            linha['Carga Geral Acondicionamento'],
            linha['ConteinerEstado'],
            linha['Tipo Navegação'],
            linha['FlagAutorizacao'],
            linha['FlagCabotagem'],
            linha['FlagCabotagemMovimentacao'],
            linha['FlagConteinerTamanho'],
            linha['FlagLongoCurso'],
            linha['FlagMCOperacaoCarga'],
            linha['FlagOffshore'],
            linha['FlagTransporteViaInterioir'],
            linha['Percurso Transporte em vias Interiores'],
            linha['Percurso Transporte Interiores'],
            linha['STNaturezaCarga'],
            linha['STSH2'],
            linha['STSH4'],
            linha['Natureza da Carga'],
            linha['Sentido'],
            linha['TEU'],
            linha['QTCarga'],
            linha['VLPesoCargaBruta']
        )
        con.commit()

    cursor.close()
    con.close()


def importa_tbl_atracacao():
    con = conexao_db('LAKE_RAW')
    cursor = con.cursor()
    dados = importa_arq('Atracacao.txt')
    dados.fillna('', inplace=True)

    for indice, linha in dados.iterrows():
        cursor.execute(
            'INSERT INTO [dbo].[TBL_ATRACACAO](     ' +
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
            linha['IDAtracacao'],
            linha['CDTUP'],
            linha['IDBerco'],
            linha['Berço'],
            linha['Porto Atracação'],
            linha['Apelido Instalação Portuária'],
            linha['Complexo Portuário'],
            linha['Tipo da Autoridade Portuária'],
            linha['Data Atracação'],
            linha['Data Chegada'],
            linha['Data Desatracação'],
            linha['Data Início Operação'],
            linha['Data Término Operação'],
            linha['Ano'],
            linha['Mes'],
            linha['Tipo de Operação'],
            linha['Tipo de Navegação da Atracação'],
            linha['Nacionalidade do Armador'],
            linha['FlagMCOperacaoAtracacao'],
            linha['Terminal'],
            linha['Município'],
            linha['UF'],
            linha['SGUF'],
            linha['Região Geográfica'],
            linha['Nº da Capitania'],
            linha['Nº do IMO']
        )
        con.commit()

    cursor.close()
    con.close()


importa_tbl_carga()
importa_tbl_atracacao()

# print(importa_arq('2021Atracacao.txt').head())
