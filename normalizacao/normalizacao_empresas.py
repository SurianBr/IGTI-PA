import sys
import pandas
import numpy as np
from pathlib import Path
from normalizador_df_pandas import Normalizador_df_pandas

'''
    Normaliza o CSV de empresas
'''
# prepara normalizador
normalizador = Normalizador_df_pandas('normalizacao/colunas/colunas_empresas.json')

# Caminhos arquivos
caminho_entrada = 'arquivos/raw/empresas/empresas.csv'
caminho_saida = 'arquivos/har/empresas/'

# Cria as pastas se não existir
Path(caminho_saida).mkdir(parents=True, exist_ok=True)

# Gera df pandas do csv.
df_pandas = pandas.read_csv(
    caminho_entrada,
    sep=',',
    skiprows=2,
    header=None
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

df = retorno

df['ativo'] = df['ativo'].replace(
    [
        'Y'
    ],
    'S'
)

# Remove fim de linha
df = df.replace(r'\\N', np.nan, regex=True)

# Adicona dados da Varig
df.loc[len(df.index)] = [999999, 'Varig Linhas Aereas', 'Varig', 'RG', 'VRG', 'VARIG', 'Brazil', 'N']

# Colocar "N" pna coluna ativa para companinhas que não existem mais
df.iat[df.index[df['icao'] == 'VRN'].to_list()[0], 7] = 'N'
df.iat[df.index[df['icao'] == 'VSP'].to_list()[0], 7] = 'N'
df.iat[df.index[df['icao'] == 'TIB'].to_list()[0], 7] = 'N'

novo_nome_arquivo = 'empresas.snappy.parquet'

# Grava parquet
df.to_parquet(caminho_saida + '{0}'.format(novo_nome_arquivo))
