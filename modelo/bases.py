from pyspark.sql import SparkSession

class Bases():
    '''
        Carrega as bases e processa as query para o servidor
    '''

    def __init__(self) -> None:

        # Prepara sessao
        self.spark = SparkSession.builder.appName("processa_querys").getOrCreate()

        # Carrega bases
        self.vra = self.spark.read.parquet('arquivos/res/vra/vra_final.snappy.parquet')
        self.vra.createOrReplaceTempView('vra')

        self.aerodromos = self.spark.read.parquet('arquivos/har/aerodromos/aerodromos.snappy.parquet')
        self.aerodromos.createOrReplaceTempView('aerodromos')


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


    def gerar_dados_home_page(self):
        '''
            Gera os dados necessarios para popular a home page

            Retono:
                Dados (Dataframe Spark): Dataframe com os dados para a home page
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
