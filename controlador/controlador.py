
from asyncore import read
import cgi
from mimetypes import init
import socketserver
from http.server import BaseHTTPRequestHandler
from modelo.bases import Bases
from visualizacao.paginas import Paginas

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

        if (caminho.endswith('favicon.ico')):
            self.processa_get_favicon()

        if (caminho.endswith('/')):
            self.processa_get_home_page()
        
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
