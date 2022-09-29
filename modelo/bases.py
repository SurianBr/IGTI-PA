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
        '''

        if self.dados_home is None:

            self.dados_home = []
            
            self.dados_home.append(self.gerar_dados_home_page_total_voos())
            self.dados_home.append(self.gerar_dados_home_page_voos_ano())

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
