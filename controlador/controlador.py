
import os
import cgi
import shutil
from http.server import BaseHTTPRequestHandler

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

        elif caminho.endswith('metadados'):
            self.processa_get_metadados()

        elif caminho.find('download-vra') > 0:
            self.processa_get_download_vra()

        elif caminho.find('download-aerodromos') > 0:
            self.processa_get_download_aerodromos()
        
        '''
        if self.path.endswith('/'):
            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()
            self.wfile.write(str(self.home_page).encode())
            
        else:
            self.send_response(404)
            self.send_header('content-type', 'text/html')
            self.end_headers()
        '''


    def do_POST(self):
        self.send_response(200)
        self.send_header('content-type', 'text/html')
        self.end_headers()
        self.wfile.write('Hello World'.encode())

        print("POST")

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
