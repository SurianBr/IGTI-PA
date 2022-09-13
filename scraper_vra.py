import re
import urllib.request
import json
from bs4 import BeautifulSoup


#### Busca dados abertos da ANAC sobre Voos Regulares Ativos ###


# URL dos Dados
url = 'https://sistemas.anac.gov.br/dadosabertos/Voos%20e%20opera%C3%A7%C3%B5es%20a%C3%A9reas/Voo%20Regular%20Ativo%20%28VRA%29/'

# Regex para pegar soment os links dos anos
pattern_ano = re.compile("^\d{4}/$") # '2022/' -> ok

# Busca pagina
page = urllib.request.urlopen(url)
soup = BeautifulSoup(page.read(), 'html.parser')

# Busca links de cada ano
links_ano = []
for link in soup.find_all('a'):
    href = link.get('href')

    # Busca pattern para não pegar o link de 'voltar'
    if re.search(pattern_ano, href) is not None:
        links_ano.append(
            {
                'data': href[0:-1],
                'link': url + href
            }
        )

# Pega os links dos meses de todos os anos disponiveis
pattern_mes = re.compile("^\d{2}\s-\s\w+/$") # '01 - Janeiro/' -> ok

links_mes = []
for item in links_ano:
    page = urllib.request.urlopen(item['link'])
    soup = BeautifulSoup(page.read(), 'html.parser')

    for link in soup.find_all('a'):
        href = link.get('href')
        texto = link.string

        # Busca pattern para não pegar o link de 'voltar'
        if re.search(pattern_mes, texto) is not None:
            links_mes.append(
                {
                    'data': item['data'] + texto[0:2],
                    'link': item['link'] + href
                }
            )

# Busca os links para o download dos CSVs
links_csv = []
for item in links_mes:
    page = urllib.request.urlopen(item['link'])
    soup = BeautifulSoup(page.read(), 'html.parser')

    for link in soup.find_all('a'):
        href = link.get('href')

        # Verifica se arquivo é um CSV
        if href.find('.csv') != -1:
            # Transforma data de 202201 para 20221 - datas no CSVs não tem 0 nos meses
            data_mes_sem_zeros = item['data'][0:4] + item['data'][-2:].replace('0', '') 

            # Verifica se a data do CSVs é igual a data esperada
            if (href.find(item['data']) != -1 or href.find(data_mes_sem_zeros) != -1): 
                links_csv.append(
                    {
                        'data': item['data'],
                        'link': item['link'] + href
                    }
                )

# Grava lista de arquivos
saida = open('lista_vra.json', 'w', encoding='latin-1')
saida.write(json.dumps(links_csv))
saida.close()

