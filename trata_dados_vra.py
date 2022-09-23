import pandas
from pathlib import Path

'''
    Tratos os dados de Voos Regulares Ativos elimando inconsistencias
'''

# Carrega base inteira
df = pandas.read_parquet('arquivos/har/vra/')

# Trata icao_empresa_aerea
df['icao_empresa_aerea'] = df['icao_empresa_aerea'].replace(['010', '060'], None)

# Trata codigo_autorizacao 
df['codigo_autorizacao'] = df['codigo_autorizacao'].replace(
    [
        '0.0',
        '1.0',
        '2.0',
        '3.0',
        '4.0',
        '5.0',
        '6.0',
        '7.0',
        '8.0',
        '9.0',
        '?'
    ],
    [
        '0',
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        None
    ]
)

# Trata icao_aerodromo_origem 
df['icao_aerodromo_origem'] = df['icao_aerodromo_origem'].replace(
    [
        '????',
        'X',
        '0',
        '0GMM',
        '1AON',
        '1SBB',
        '1SBE',
        '2NHT',
        '4CEL',
        '6ASO',
        'PGF',
        'SB',
        'SOW'
    ],
    None
)

# Trata icao_aerodromo_destino 
df['icao_aerodromo_destino'] = df['icao_aerodromo_destino'].replace(
    [
        '????',
        '1AON',
        '2NHT',
        '6ASO',
        'BEG '
    ],
    None
)


# Trata data_partida_prevista 
df['data_partida_prevista'] = df['data_partida_prevista'].replace(
    [
        'Microsoft.SqlServer.Dts.Pipeline.BlobColumn'
    ],
    None
)

# Trata data_partida_real 
df['data_partida_real'] = df['data_partida_real'].replace(
    [
        'Microsoft.SqlServer.Dts.Pipeline.BlobColumn'
    ],
    None
)

# Trata data_chegada_prevista 
df['data_chegada_prevista'] = df['data_chegada_prevista'].replace(
    [
        'Microsoft.SqlServer.Dts.Pipeline.BlobColumn'
    ],
    None
)

# Trata data_chegada_real 
df['data_chegada_real'] = df['data_chegada_real'].replace(
    [
        'Microsoft.SqlServer.Dts.Pipeline.BlobColumn'
    ],
    None
)

# Trata situacao_voo 
df['situacao_voo'] = df['situacao_voo'].replace(
    [
        'Microsoft.SqlServer.Dts.Pipeline.BlobColumn'
    ],
    None
)

# Trata codigo_justificativa 
df['codigo_justificativa'] = df['codigo_justificativa'].replace(
    [
        '??'
    ],
    None
)

caminho = 'arquivos/res/vra/'

# Cria as pastas se não existir
Path(caminho).mkdir(parents=True, exist_ok=True)

df.to_parquet(caminho + 'vra_final.snappy.parquet')
