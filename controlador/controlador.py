import os
import cgi
import shutil
import io
from urllib.parse import urlparse
from urllib.parse import parse_qs
from http.server import BaseHTTPRequestHandler
from datetime import datetime

class Controlador(BaseHTTPRequestHandler):
    '''
        Controla todas as requisições recebidas pelo servidor
    '''

    #def __init__(self, request: bytes, client_address: tuple[str, int], server: socketserver.BaseServer) -> None:
    def __init__(self, bases, paginas, *args, **kwargs):
        self.bases = bases
        self.paginas = paginas

        # Inicializa variaveis
        self.favicon = None

        super().__init__(*args, **kwargs)

    
    def do_GET(self):
        '''
            Processa requisicoes de get.
            Verifica qual pagina foi chamada e chama metodo para o processamento 
        '''
        caminho = self.path

        if caminho.endswith('favicon.ico'):
            self.processa_get_favicon()

        elif caminho.endswith('/'):
            self.processa_get_home_page()

        elif caminho.find('consulta') > 0:
            parsed_url = urlparse(caminho)
            captured_value = parse_qs(parsed_url.query)
            query = captured_value.get('query', None)
            self.processa_get_consulta(query)

        elif caminho.find('metadados') > 0:
            self.processa_get_metadados()

        elif caminho.find('download-vra') > 0:
            self.processa_get_download_vra()

        elif caminho.find('download-aerodromos') > 0:
            self.processa_get_download_aerodromos()

        elif caminho.find('download-empresas') > 0:
            self.processa_get_download_empresas()

        elif caminho.find('download-query') > 0:
            parsed_url = urlparse(caminho)
            captured_value = parse_qs(parsed_url.query)
            query = captured_value.get('query', None)

            self.processa_get_download_query(query[0])


    def do_POST(self):
        self.send_response(200)
        self.send_header('content-type', 'text/html')
        self.end_headers()
        self.wfile.write('Hello World'.encode())

        ctypes, pdict = cgi.parse_header(self.headers.get('content-type'))


    def processa_get_favicon(self):
        '''
            Processa requisicoes get para o favicon
        '''

        self.send_response(200)
        self.send_header('content-type', 'image/jpeg')
        self.end_headers()
        self.wfile.write(self.bases.busca_favicon())


    def processa_get_home_page(self):
        '''
            Processa requisicoes para a home page
        '''
        dados = self.bases.busca_dados_home_page()

        self.send_response(200)
        self.send_header('content-type', 'text/html')
        self.end_headers()
        self.wfile.write(str(self.paginas.get_home_page(dados)).encode())

    
    def processa_get_consulta(self, query):
        '''
            Processa requisicoes para a pagina de consulta
        '''

        if query is None:
            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()
            self.wfile.write(str(self.paginas.get_consulta()).encode())

        elif query[0].replace(' ', '').replace('|', '') == '':
            self.send_response(302)
            self.send_header('Location', '/consulta')
            self.end_headers()

        else:
            codigo, mensagem, dados = self.bases.executar_query(query[0].replace('|', ' '))

            if codigo != 0:
                raise Exception(mensagem)
 
            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()
            self.wfile.write(str(self.paginas.get_consulta_query(dados, query[0])).encode())


    def processa_get_metadados(self):
        '''
            Processa requisicoes para a pagina de metadados
        '''

        self.send_response(200)
        self.send_header('content-type', 'text/html')
        self.end_headers()
        self.wfile.write(str(self.paginas.get_metadados()).encode())


    def processa_get_download_vra(self):
        '''
            Processa requisicoes para o download da tabela de VRA
        '''

        with open('arquivos/res/vra/vra_final.snappy.parquet', 'rb') as f:
            fs = os.fstat(f.fileno())
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Disposition", 'attachment; filename="vra_final.snappy.parquet"')
            self.send_header("Content-Length", str(fs.st_size))
            self.end_headers()
            shutil.copyfileobj(f, self.wfile)


    def processa_get_download_aerodromos(self):
        '''
            Processa requisicoes para o download da tabela de aerodromos
        '''

        with open('arquivos/har/aerodromos/aerodromos.snappy.parquet', 'rb') as f:
            fs = os.fstat(f.fileno())
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Disposition", 'attachment; filename="aerodromos.snappy.parquet"')
            self.send_header("Content-Length", str(fs.st_size))
            self.end_headers()
            shutil.copyfileobj(f, self.wfile)


    def processa_get_download_empresas(self):
        '''
            Processa requisicoes para o download da tabela de empresas
        '''

        with open('arquivos/har/empresas/empresas.snappy.parquet', 'rb') as f:
            fs = os.fstat(f.fileno())
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Disposition", 'attachment; filename="empresas.snappy.parquet"')
            self.send_header("Content-Length", str(fs.st_size))
            self.end_headers()
            shutil.copyfileobj(f, self.wfile)


    def processa_get_download_query(self, query):

        # data processamento em utc
        data_hora = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')

        # Gera nome do arquivo
        arquivo = 'resultados_query/query_{0}.csv'.format(data_hora)

        # Repete a query para fazer o download
        codigo, mensagem, dados = self.bases.executar_query(query.replace('|', ' '))

        if codigo != 0:
            raise Exception(mensagem)

        # Grava o parquet em memoria
        dados.toPandas().to_csv(arquivo, encoding='utf-8')

        # Manda resposta
        with open(arquivo, 'rb') as f:
            fs = os.fstat(f.fileno())
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Disposition", 'attachment; filename="{0}"'.format(arquivo))
            self.send_header("Content-Length", str(fs.st_size))
            self.end_headers()
            shutil.copyfileobj(f, self.wfile)