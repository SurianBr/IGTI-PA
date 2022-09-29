from functools import partial
from http.server import HTTPServer
from modelo.bases import Bases
from visualizacao.paginas import Paginas
from controlador.controlador import Controlador

'''
    Inicia o servidor HTTPS
'''

def main():

    # Preparacao
    bases = Bases()
    paginas = Paginas()

    # Cria Controlador parcial com as bases e paginas
    controlador = partial(Controlador, bases, paginas)

    # Inicia o servidor
    port = 8000
    server = HTTPServer(('', port), controlador)
    server.serve_forever()


if __name__ == '__main__':
    main()