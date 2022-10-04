import re
from pyspark.sql import SparkSession

class Bases():
    '''
        Carrega as bases e processa as query para o servidor
    '''

    def __init__(self) -> None:

        # Prepara variaveis
        self.favicon = None
        self.dados_home = None

        # Prepara sessao
        self.spark = SparkSession.builder.appName("bases").getOrCreate()

        # Carrega bases
        self.vra = self.spark.read.parquet('arquivos/res/vra/vra_final.snappy.parquet')
        self.vra.createOrReplaceTempView('vra')

        self.aerodromos = self.spark.read.parquet('arquivos/har/aerodromos/aerodromos.snappy.parquet')
        self.aerodromos.createOrReplaceTempView('aerodromos')


    def busca_favicon(self):
        if self.favicon is None:
            self.favicon = open('favicon.ico', 'rb')
        else:
            self.favicon.seek(0)

        return self.favicon.read()


    def executar_query(self, query):
        '''
            Executa a query passado no paremetro

            Parametros:
                query (String): SQL query a ser executada

            Retorno:
                codigo (int): 0
                    mensagem (string): Processmento ok
                    df (DataFrame): Retorna resultado da query em um dataframe spark

                codigo (int): -1
                    mensagem (string): Mensagem do exeption
                    df (DataFrame): Retorna None
        '''
        
        codigo = 0
        mensagem = 'Processmento ok.'

        try:
            df = self.spark.sql(query)

            return codigo, mensagem, df

        except Exception as e:
            codigo = -1
            mensagem = '{0}'.format(e)

            return codigo, mensagem, None


    def busca_dados_home_page(self):
        '''
            Gera os dados necessarios para popular a home page

            Retono:
                Dados (Lista): Lista de dataframes com os dados para a home page
                    Posição:
                        0: Numero total de voos
                        1: Numero total de voos por ano
                        2: Top 20 rotas
                        3: Top 20 aerodromos
        '''

        if self.dados_home is None:

            self.dados_home = []
            
            self.dados_home.append(self.gerar_dados_home_page_total_voos())
            self.dados_home.append(self.gerar_dados_home_page_voos_ano())
            self.dados_home.append(self.gerar_dados_home_top_20_rotas())
            self.dados_home.append(self.gerar_dados_home_top_20_aerodromos())

        return self.dados_home 


    def gerar_dados_home_page_total_voos(self):
        '''
            Gera os dado com o total de voos para a home page
            Retono:
                Dados (Dataframe Spark)
        '''
        
        query = (
            ' SELECT COUNT(1) as quantidade_voos'
            ' FROM vra'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df
        
        else:
            raise Exception(mensagem)


    def gerar_dados_home_page_voos_ano(self):
        '''
            Gera os dados de voos por ano para a home page
            Retono:
                Dados (Dataframe Spark)
        '''
        
        query = (
            ' SELECT ano_voo, COUNT(1) as quantidade_voos'
            ' FROM vra'
            ' GROUP BY ano_voo'
            ' ORDER BY ano_voo ASC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df
        
        else:
            raise Exception(mensagem)



    def gerar_dados_home_top_20_rotas(self):
        '''
            Gera os dados das top 20 rotas mais voadas
            Retono:
                Dados (Dataframe Spark)
        '''
        
        # Calcula quantidade de voos por rota
        query = (
            ' SELECT icao_aerodromo_origem, icao_aerodromo_destino,  COUNT(1) as quantidade_voos'
            ' FROM vra'
            ' GROUP BY icao_aerodromo_origem, icao_aerodromo_destino'
            ' ORDER BY quantidade_voos DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('rotas')
        
        else:
            raise Exception(mensagem)

        # Busca nome do aerodromo e cidade da origem
        query = (
            ' SELECT aerodromos.nome as aerodromo_origem, aerodromos.municipio as municipio_origem, icao_aerodromo_destino, quantidade_voos'
            ' FROM rotas'
            ' INNER JOIN aerodromos'
            ' ON rotas.icao_aerodromo_origem = aerodromos.icao'
            ' ORDER BY quantidade_voos DESC '
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('rotas_origem')
        
        else:
            raise Exception(mensagem)

        # Busca nome do aerodromo e cidade de destino e limita para os 20 primeiros
        query = (
            ' SELECT aerodromo_origem, municipio_origem, aerodromos.nome as aerodromo_destino, aerodromos.municipio as municipio_destino, quantidade_voos'
            ' FROM rotas_origem'
            ' INNER JOIN aerodromos'
            ' ON rotas_origem.icao_aerodromo_destino = aerodromos.icao'
            ' ORDER BY quantidade_voos DESC '
            ' LIMIT 20'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df
        
        else:
            raise Exception(mensagem)

    def gerar_dados_home_top_20_aerodromos(self):
        '''
            Gera os dados de voos por ano para a home page
            Retono:
                Dados (Dataframe Spark)
        '''
        
        query = (
            ' SELECT icao_aerodromo_origem, COUNT(1) as decolagens'
            ' FROM vra'
            ' GROUP BY icao_aerodromo_origem'
            ' ORDER BY decolagens DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('decolagens')
        
        else:
            raise Exception(mensagem)

        query = (
            ' SELECT icao_aerodromo_destino, COUNT(1) as aterrisagens'
            ' FROM vra'
            ' GROUP BY icao_aerodromo_destino'
            ' ORDER BY aterrisagens DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('aterrisagens')
        
        else:
            raise Exception(mensagem)

        query = (
            ' SELECT decolagens.icao_aerodromo_origem as aerodromo, decolagens, aterrisagens, (decolagens+aterrisagens) as total'
            ' FROM decolagens'
            ' INNER JOIN aterrisagens'
            ' ON decolagens.icao_aerodromo_origem = aterrisagens.icao_aerodromo_destino'
            ' ORDER BY total DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('aerodromos_mais_usados')
        
        else:
            raise Exception(mensagem)

        query = (
            ' SELECT nome, municipio, decolagens, aterrisagens, total'
            ' FROM aerodromos_mais_usados'
            ' INNER JOIN aerodromos'
            ' ON aerodromo = icao'
            ' AND pais_iso = "BR"'
            ' ORDER BY total DESC'
            ' LIMIT 20'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df
        
        else:
            raise Exception(mensagem)