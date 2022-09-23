import os
import json
import sys
import pandas
from pathlib import Path
from normalizador_df_pandas import Normalizador_df_pandas

'''
    Normaliza os arquivos CSV de VRA e grava em parquet
'''

normalizador = Normalizador_df_pandas('normalizacao/colunas/colunas_vra.json')

# Busca caminho absoluto
caminho_absoluto = os.path.abspath('arquivos/raw/vra/')
caminho_absoluto = caminho_absoluto.replace('\\', '/') + '/'

#Busca lista de arquivos
lista_arquivos = os.listdir(caminho_absoluto)

# Onde os arquivos serão gravados
caminho = 'arquivos/har/vra/'

# Cria as pastas se não existir
Path(caminho).mkdir(parents=True, exist_ok=True)

# Normalização dos arquivos em csv e gravação em parquet
for arquivo in lista_arquivos:

    print('Normalizando arquivo {0}'.format(arquivo))

    # Gera df pandas do csv - elimina a primeira linha (data de geração) e
    # utiliza a segunda linha para o nome das colunas
    df_pandas = pandas.read_csv(
        caminho_absoluto + arquivo,
        sep=';',
        skiprows=1,
        header=0
    )

    # Normaliza
    codigo, retorno = normalizador.normalizar(df_pandas)

    # Trata retorno da normalização
    if codigo == -1:
        print('Erro na normalização')
        print(retorno)
        print('Colunas Encontradas: \n{0}'.format(
            df_pandas.columns.values.tolist()
        ))

        sys.exit(-1)

    df_pandas_normalizado = retorno

    # Força tipo string para algumas colunas
    df_pandas_normalizado = df_pandas_normalizado.astype(
        {'numero_voo': 'string', 'codigo_autorizacao': 'string'}
    )

    # Adiciona coluna com o ano do voo
    df_pandas_normalizado['ano_voo'] = arquivo[4:-6]

    # Adiciona coluna com o mes do voo
    df_pandas_normalizado['mes_voo'] = arquivo[8:-4]

    # Adiciona coluna com o nome do arquivo de origem
    df_pandas_normalizado['arquivo_origem'] = arquivo

    # Grava parquet
    novo_nome_arquivo = '{0}.snappy.parquet'.format(arquivo[:-4])
    df_pandas_normalizado.to_parquet(caminho + '{0}'.format(novo_nome_arquivo))


