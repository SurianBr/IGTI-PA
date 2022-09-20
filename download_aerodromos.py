import urllib.request
from pathlib import Path

'''
    Faz o download da lista de aerodromos

    Fonte: https://ourairports.com/
'''


url = 'https://davidmegginson.github.io/ourairports-data/airports.csv'

# Onde os arquivos serão baixados
caminho = 'arquivos/raw/aerodromos/'

# Cria pastas se não tiver
Path(caminho).mkdir(parents=True, exist_ok=True)

# Faz o download da lista
urllib.request.urlretrieve(
    url,
    '{0}/aerodromos.csv'.format(
        caminho,
    )
)