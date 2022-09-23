from http.server import HTTPServer
from controlador.controlador import Controlador

'''
    Inicia o servidor HTTPS
'''

def main():
    port = 8000
    server = HTTPServer(('', port), Controlador)
    server.serve_forever()


if __name__ == '__main__':
    main()