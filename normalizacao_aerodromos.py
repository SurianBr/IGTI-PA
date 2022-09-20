import sys
import pandas
from pathlib import Path
from Normalizador_df import Normalizador_df

'''
    Normaliza o CSV de aerodromos
'''
# prepara normalizador
normalizador_df = Normalizador_df('colunas_aerodromos.json')

# Caminhos arquivos
caminho_entrada = 'arquivos/raw/aerodromos/aerodromos.csv'
caminho_saida = 'arquivos/har/aerodromos/'

# Cria as pastas se não existir
Path(caminho_saida).mkdir(parents=True, exist_ok=True)

# Gera df pandas do csv.
df_pandas = pandas.read_csv(
    caminho_entrada,
    sep=',',
    header=0
)

# Normaliza
codigo, retorno = normalizador_df.normalizar(df_pandas)

# Trata retorno da normalização
if codigo == -1:
    print('Erro na normalização')
    print(retorno)
    print('Colunas Encontradas: \n{0}'.format(
        df_pandas.columns.values.tolist()
    ))

    sys.exit(-1)

novo_nome_arquivo = 'aerodromos.snappy.parquet'

# Grava parquet
retorno.to_parquet(caminho_saida + '{0}'.format(novo_nome_arquivo))
