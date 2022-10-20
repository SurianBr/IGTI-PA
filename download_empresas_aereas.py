import urllib.request
from pathlib import Path

'''
    Faz o download da lista de empresas aereas

    Fonte: https://openflights.org/data.html#airline
'''


url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat'

# Onde os arquivos serão baixados
caminho = 'arquivos/raw/empresas/'

# Cria pastas se não tiver
Path(caminho).mkdir(parents=True, exist_ok=True)

# Faz o download da lista
urllib.request.urlretrieve(
    url,
    '{0}/empresas.csv'.format(
        caminho,
    )
)