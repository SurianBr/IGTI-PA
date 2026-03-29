import subprocess
import time
import json

class ConfigVRA:
    '''
    Configura os itens necessarions para o download, normalizacao e carga no mysql do VRA (Voo Regular Ativo).
    '''

    def __init__(self, passos=None):

        # Passos padrão para o processo de configuração do VRA completo
        passos_padrao = [
            'instalar_dependencias', 
            'executar_scraper',
            'download_vra',
            'download_aerodromos',
            'download_empresas',
            'normalizar_vra',
            'normalizar_aerodromos',
            'normalizar_empresas',
            'tratar_inconsistencias_vra',
            'criar_database_mysql'
        ]
        self.passos = passos or passos_padrao

        self.database_name = 'vra_db'


    def iniciar(self):
        print('Iniciando configuração do VRA...')

        if 'instalar_dependencias' in self.passos:
            self.instalar_dependencias()

        if 'executar_scraper' in self.passos:
            self.executar_scraper()

        if 'download_vra' in self.passos:
            self.download_vra()

        if 'download_aerodromos' in self.passos:
            self.download_aerodromos()

        if 'download_empresas' in self.passos:
            self.download_empresas()

        if 'normalizar_vra' in self.passos:
            self.normalizar_vra()

        if 'normalizar_aerodromos' in self.passos:
            self.normalizar_aerodromos()

        if 'normalizar_empresas' in self.passos:
            self.normalizar_empresas()

        if 'tratar_inconsistencias_vra' in self.passos:
            self.tratar_inconsistencias_vra()

        if 'criar_database_mysql' in self.passos:
            host, user, password = self.buscar_credenciais_mysql()

            from config_mysql.mySqlutils import MySQLUtils

            mysql_utils = MySQLUtils()

            try:
                mydb = mysql_utils.conectar(host, user, password)
        
            except Exception as e:
                print(f'Erro ao conectar no MySQL: {e}')
                raise e
            
            if 'criar_database_mysql' in self.passos:
                self.criar_database_mysql(mydb, host, user, password)

            mydb.close()

        print('Configuração do VRA concluída com sucesso.')


    def instalar_dependencias(self):
        '''Instala as dependencias do projeto'''
        print('Instalando dependências...')
        try:
            subprocess.run(['pip', 'install', '-r', 'requirements.txt'])
        except Exception as e:
            print(f'Erro ao instalar dependências: {e}')
            raise e
        
        print('Dependências instaladas com sucesso.')


    def executar_scraper(self):
        '''Executa o scraper para buscar os links dos arquivos CSV do VRA'''
        print('Executando scraper do VRA...')

        import scraper.scraper_vra as scraper_vra

        try:
            scraper_vra.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                scraper_vra.executar()
            except Exception as e:
                print(f'Erro ao executar o scraper: {e}')
                raise e
            
        print('Scraper do VRA executado com sucesso.')


    def download_vra(self):
        '''Executa o processo de download dos arquivos CSV do VRA'''
        print('Executando download dos arquivos CSV do VRA...')

        from download import download_vra

        try:
            download_vra.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                download_vra.executar()
            except Exception as e:
                print(f'Erro ao executar o download: {e}')
                raise e

        print('Download dos arquivos CSV do VRA executado com sucesso.')


    def download_aerodromos(self):
        '''Executa o processo de download dos arquivos CSV de aerodromos'''
        print('Executando download dos arquivos CSV de aerodromos...')

        from download import download_aerodromos

        try:
            download_aerodromos.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                download_aerodromos.executar()
            except Exception as e:
                print(f'Erro ao executar o download: {e}')
                raise e

        print('Download dos arquivos CSV do aerodromos executado com sucesso.')


    def download_empresas(self):
        '''Executa o processo de download dos arquivos CSV de Empresas Aéreas'''
        print('Executando download dos arquivos CSV de empresas aereas...')

        from download import download_empresas_aereas

        try:
            download_empresas_aereas.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                download_empresas_aereas.executar()
            except Exception as e:
                print(f'Erro ao executar o download: {e}')
                raise e

        print('Download dos arquivos CSV de empresas aereas executado com sucesso.')


    def normalizar_vra(self):
        '''Executa o processo de normalização dos arquivos CSV do VRA'''
        print('Executando normalização dos arquivos CSV do VRA...')

        from normalizacao import normalizacao_vra

        try:
            normalizacao_vra.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                normalizacao_vra.executar()
            except Exception as e:
                print(f'Erro ao executar a normalização: {e}')
                raise e

        print('Normalização dos arquivos CSV do VRA executada com sucesso.')


    def normalizar_aerodromos(self):
        '''Executa o processo de normalização dos arquivos CSV de aerodromos'''
        print('Executando normalização dos arquivos CSV de aerodromos...')

        from normalizacao import normalizacao_aerodromos

        try:
            normalizacao_aerodromos.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                normalizacao_aerodromos.executar()
            except Exception as e:
                print(f'Erro ao executar a normalização: {e}')
                raise e

        print('Normalização dos arquivos CSV de aerodromos executada com sucesso.')


    def normalizar_empresas(self):
        '''Executa o processo de normalização dos arquivos CSV de empresas aereas'''
        print('Executando normalização dos arquivos CSV de empresas aereas...')

        from normalizacao import normalizacao_empresas

        try:
            normalizacao_empresas.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                normalizacao_empresas.executar()
            except Exception as e:
                print(f'Erro ao executar a normalização: {e}')
                raise e

        print('Normalização dos arquivos CSV de empresas aereas executada com sucesso.')

    
    def tratar_inconsistencias_vra(self):
        '''Executa o processo de tratamento dos dados do VRA eliminando inconsistencias'''
        print('Executando tratamento das inconsistencias do VRA...')

        from resultado import trata_dados_vra

        try:
            trata_dados_vra.executar()
        except Exception as e:
            # Em caso de erro, espera 5 segundos e tenta novamente
            time.sleep(5)
            try:
                trata_dados_vra.executar()
            except Exception as e:
                print(f'Erro ao executar o tratamento: {e}')
                raise e

        print('Tratamento das inconsistencias do VRA executado com sucesso.')


    def buscar_credenciais_mysql(self):
        '''Busca as credenciais do MySQL para conexão'''
        print('Buscando credenciais do MySQL...')

        try:
            with open('credenciais.json', 'r') as f:
                credenciais = json.load(f)
                host = credenciais.get('host')
                user = credenciais.get('user')
                password = credenciais.get('password')

                if not host or not user or not password:
                    raise ValueError("Credenciais incompletas no arquivo 'credenciais.json'.")

                return host, user, password

        except Exception as e:
            print(f'Erro ao buscar credenciais do MySQL: {e}')
            raise e


    def criar_database_mysql(self, mydb, host, user, password):
        '''Cria o banco de dados no MySQL usando as credenciais fornecidas'''
        print('Criando database no MySQL...')

        from config_mysql.criarDatabase import CriarDatabase

        try:
            criar_db = CriarDatabase(mydb, host, user, password, self.database_name)
            criar_db.criar_database()
        except Exception as e:
            print(f'Erro ao criar database no MySQL: {e}')
            raise e

        print('Database criada com sucesso no MySQL.')


if __name__ == '__main__':
    
    passos_para_executar = [
        'instalar_dependencias',
        'criar_database_mysql'

    ]
    config = ConfigVRA(passos_para_executar)

    #config = ConfigVRA()
    config.iniciar()
