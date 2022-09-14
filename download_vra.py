import sys
import urllib.request
import json

'''
    Faz o download dos arquivos de Voos Regulares Ativos
    a partir da lista gerada pelo scraper
'''

# Onde os arquivos serão baixados
caminho = 'arquivos/raw/vra/'

# Arquivos das listas
lista_vra_arquivo = None
lista_vra_baixados_arquivo = None

# Lista dos arquivos que o scraper gerou
lista_vra = None

# Lista com os arquivos que já foram baixados
lista_vra_baixados = []

# Lista com todos os arquivos que serão baixados
lista_final_vra = []

# Busca lista com os links
try:
    lista_vra_arquivo = open('lista_vra.json', 'r', encoding='latin-1')
except FileNotFoundError as e:
    print('Lista de arquivo do VRA não encontrato')
    sys.exit(-1)
else:
    lista_vra_string = lista_vra_arquivo.read()
    
    # Trata arquivo vazio
    if lista_vra_string is not None:
        lista_vra = json.loads(lista_vra_string)
        lista_vra_arquivo.close()
    else:  # Encerra processamento
        print('Lista de arquivo do VRA está vazia')
        sys.exit(-1)

# Busca lista dos arquivos que
# já foram baixados
try:
    lista_vra_baixados_arquivo = open('lista_vra_baixados.json', 'r', encoding='latin-1')

except FileNotFoundError:
    pass  # contina processamento com uma lista vazia

else:
    lista_vra_baixados_arquivo_string = lista_vra_baixados_arquivo.read()

    # Trata arquivo vazio
    if lista_vra_baixados_arquivo_string is not None:
        lista_vra_baixados = json.loads(lista_vra_baixados_arquivo_string)

    lista_vra_baixados_arquivo.close()

# Retira da lista arquivo que já foi feito o download
for arquivo in lista_vra:
    feito_baixados = False

    for download in lista_vra_baixados:
        if download['data'] == arquivo['data']:
            feito_baixados = True
            break

    if feito_baixados is False:
        lista_final_vra.append(arquivo)


# Faz o download dos arquivos
falha = False
try:
    for arquivo in lista_final_vra:

        print('Fazendo download do vra_{0}.csv'.format(arquivo['data']))

        urllib.request.urlretrieve(
            arquivo['link'],
            '{0}/vra_{1}.csv'.format(
                caminho,
                arquivo['data']
            )
        )

        #adiciona arquivo na lista de baixados
        lista_vra_baixados.append(arquivo)
    
except BaseException as e:
    print('Falha no download: {0}'.format(e))

finally:
    # Trata lista vazia
    if len(lista_vra_baixados) > 0:
        lista_vra_baixados_arquivo = open('lista_vra_baixados.json', 'w', encoding='latin-1')
        lista_vra_baixados_arquivo.write(json.dumps(lista_vra_baixados))
        lista_vra_baixados_arquivo.close()

if falha is True:
    sys.exit(-1)
