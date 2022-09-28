
from asyncore import read
import cgi
import socketserver
from http.server import BaseHTTPRequestHandler
from modelo.bases import Bases
from visualizacao.paginas import Paginas

class Controlador(BaseHTTPRequestHandler):
    '''
        Controla todas as requisições recebidas pelo self
    '''

    def __init__(self, request: bytes, client_address: tuple[str, int], server: socketserver.BaseServer) -> None:

        self.prepara_bases()
        self.prepara_paginas()

        # Inicializa variaveis
        self.favicon = None

        super().__init__(request, client_address, server)


    def prepara_bases(self):
        '''
            Prepara as bases que irao ser usadas pelo o servidor
        '''
        self.bases = Bases()


    def prepara_paginas(self):
        '''
            Prepara o montador de paginas e prepara a home page
        '''
        self.paginas = Paginas()

        # Monta homepage
        self.home_page = self.paginas.get_pagina_vazia()

        self.home_page.body(
            self.paginas.get_barra_navegacao(),
            self.paginas.get_body_home(self.bases.gerar_dados_home_page()),
            self.paginas.get_rodape()
        )

    
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
        if self.favicon  == None:
            self.favicon = open('favicon.ico', 'rb')

        self.send_response(200)
        self.send_header('content-type', 'image/jpeg')
        self.end_headers()
        self.wfile.write(self.favicon.read())

        self.favicon.seek(0)




    def processa_get_home_page(self):
        '''
            Processa requisicoes para a home page
        '''
        self.send_response(200)
        self.send_header('content-type', 'text/html')
        self.end_headers()
        self.wfile.write(str(self.home_page).encode())
