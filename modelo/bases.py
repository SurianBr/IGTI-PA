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
        self.dados_cancelamento = None

        # Prepara sessao
        self.spark = SparkSession.builder.appName("bases").getOrCreate()

        # Carrega bases
        self.vra = self.spark.read.parquet('arquivos/res/vra/vra_final.snappy.parquet')
        self.vra.createOrReplaceTempView('vra')

        self.aerodromos = self.spark.read.parquet('arquivos/har/aerodromos/aerodromos.snappy.parquet')
        self.aerodromos.createOrReplaceTempView('aerodromos')

        self.empresas = self.spark.read.parquet('arquivos/har/empresas/empresas.snappy.parquet')
        self.empresas.createOrReplaceTempView('empresas')

        self.gerar_dados_cancelamentos_base()

        self.cancelamentos = None


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


    def busca_dados_cancelamentos(self):
        '''
            Gera os dados necessarios para popular a home page

            Retono:
                Dados (Lista): Lista de dataframes com os dados para a home page
                    Posição:
                        0: Cancelamentos por ano
        '''

        if self.dados_cancelamento is None:

            self.dados_cancelamento= []
            
            df, query = self.gerar_dados_cancelamentos_ano()
            self.dados_cancelamento.append({'df': df, 'query': query})

            df, query = self.gerar_dados_cancelamentos_ano_empresa()
            self.dados_cancelamento.append({'df': df, 'query': query})

            df, query = self.gerar_dados_cancelamentos_ano_aerodromos()
            self.dados_cancelamento.append({'df': df, 'query': query})

        return self.dados_cancelamento 


    def gerar_dados_cancelamentos_base(self):
        '''
            Gera a base de dados que ira ser usada para os calculos de cancelamentos
            Retono:
                Dados (Dataframe Spark)
        '''
        query = (
            ' SELECT ano_voo, mes_voo, icao_empresa_aerea, icao_aerodromo_origem as aerodromo, COUNT(1) as quantidade_voos_total'
            ' FROM vra'
            ' WHERE icao_empresa_aerea IS NOT NULL'
            ' GROUP BY ano_voo, mes_voo, icao_empresa_aerea, icao_aerodromo_origem'
            ' ORDER BY quantidade_voos_total DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('voos_por_empresas')

        query = (
            ' SELECT ano_voo, mes_voo, icao_empresa_aerea, icao_aerodromo_origem as aerodromo, COUNT(1) as quantidade_voos_cancelados'
            ' FROM vra'
            ' WHERE situacao_voo = "CANCELADO"'
            ' AND icao_empresa_aerea IS NOT NULL'
            ' GROUP BY ano_voo, mes_voo, icao_empresa_aerea, icao_aerodromo_origem'
            ' ORDER BY quantidade_voos_cancelados DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('voos_por_empresas_cancelado')

        query = (
            ' SELECT voos_por_empresas.ano_voo, voos_por_empresas.mes_voo, voos_por_empresas.icao_empresa_aerea,'
            ' voos_por_empresas.aerodromo as aerodromo, quantidade_voos_total, quantidade_voos_cancelados'
            ' FROM voos_por_empresas'
            ' INNER JOIN voos_por_empresas_cancelado'
            ' ON voos_por_empresas.ano_voo = voos_por_empresas_cancelado.ano_voo'
            ' AND voos_por_empresas.mes_voo = voos_por_empresas_cancelado.mes_voo'
            ' AND voos_por_empresas.icao_empresa_aerea = voos_por_empresas_cancelado.icao_empresa_aerea'
            ' AND voos_por_empresas.aerodromo = voos_por_empresas_cancelado.aerodromo'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('voos_por_empresas_totais')

        query = (
            ' SELECT ano_voo, mes_voo, icao_empresa_aerea, nome, pais, aerodromo, quantidade_voos_total, quantidade_voos_cancelados'
            ' FROM voos_por_empresas_totais'
            ' INNER JOIN empresas'
            ' ON icao_empresa_aerea = icao'
            ' AND icao IS NOT NULL'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            df.createOrReplaceTempView('voos_por_empresas_final')


    def gerar_dados_cancelamentos_ano(self):
        '''
            Gera os dados de cancelamento por ano para a home page
            Retono:
                Dados (Dataframe Spark)
        '''
        
        query = (
            ' SELECT ano_voo, ((SUM(quantidade_voos_cancelados) * 100) / SUM(quantidade_voos_total)) as porcentage_cancelado'
            ' FROM voos_por_empresas_totais'
            ' GROUP BY ano_voo'
            ' ORDER BY ano_voo ASC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df, query
        
        else:
            raise Exception(mensagem)


    def gerar_dados_cancelamentos_ano_empresa(self):
        '''
            Gera os dados de cancelamento por ano e empresa
            Retono:
                Dados (Dataframe Spark)
        '''

        query = (
            ' WITH ano_empresa as ('
            ' SELECT ano_voo, icao_empresa_aerea, SUM(quantidade_voos_total) as quantidade_voos_total, SUM(quantidade_voos_cancelados) as quantidade_voos_cancelados'
            ' FROM voos_por_empresas_final'
            ' WHERE icao_empresa_aerea IN ("TAM", "GLO", "AZU")'
            ' GROUP BY ano_voo, icao_empresa_aerea'
            ')'
            'SELECT ano_voo, icao_empresa_aerea, nome, quantidade_voos_total, quantidade_voos_cancelados, '
            '  ((quantidade_voos_cancelados * 100) / quantidade_voos_total) as porcentage_cancelado'
            ' FROM ano_empresa'
            ' INNER JOIN empresas'
            ' ON icao = icao_empresa_aerea'
            ' ORDER BY ano_voo ASC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df, query
        
        else:
            raise Exception(mensagem)
        

    def gerar_dados_cancelamentos_ano_aerodromos(self):
        '''
            Gera os dados de cancelamento por ano e aerodromo
            Retono:
                Dados (Dataframe Spark)
        '''

        query = (
            ' WITH decolagens as ('
            ' SELECT icao_aerodromo_origem, COUNT(1) as decolagens'
            ' FROM vra'
            ' GROUP BY icao_aerodromo_origem'
            ' ORDER BY decolagens DESC'
            ' ), aterrisagens as ('
            ' SELECT icao_aerodromo_destino, COUNT(1) as aterrisagens'
            ' FROM vra'
            ' GROUP BY icao_aerodromo_destino'
            ' ORDER BY aterrisagens DESC'
            ' ), aerodromos_mais_usados as ('
            ' SELECT decolagens.icao_aerodromo_origem as aerodromo, decolagens, aterrisagens, (decolagens+aterrisagens) as total'
            ' FROM decolagens'
            ' INNER JOIN aterrisagens'
            ' ON decolagens.icao_aerodromo_origem = aterrisagens.icao_aerodromo_destino'
            ' ), aerodromos_mais_usados_brasil as ('
            ' SELECT aerodromos_mais_usados.*'
            ' FROM aerodromos_mais_usados'
            ' INNER JOIN aerodromos'
            ' ON aerodromos_mais_usados.aerodromo = aerodromos.icao'
            ' AND aerodromos.pais_iso = "BR"'
            ' ORDER BY total DESC'
            ' LIMIT 20'
            ' ), cancelamento_ano_aerodromo as ('
            ' SELECT ano_voo, aerodromo, SUM(quantidade_voos_total) as quantidade_voos_total, SUM(quantidade_voos_cancelados) as quantidade_voos_cancelados'
            ' FROM voos_por_empresas_final'
            ' GROUP BY ano_voo, aerodromo'
            ' ), cancelamento_ano_aerodromo_brasil ('
            ' SELECT cancelamento_ano_aerodromo.*, ((quantidade_voos_cancelados * 100) / quantidade_voos_total) as porcentage_cancelado'
            ' FROM cancelamento_ano_aerodromo'
            ' INNER JOIN aerodromos'
            ' ON cancelamento_ano_aerodromo.aerodromo = aerodromos.icao'
            ' )'
            ' SELECT cancelamento_ano_aerodromo_brasil.*, total'
            ' FROM cancelamento_ano_aerodromo_brasil'
            ' INNER JOIN aerodromos_mais_usados_brasil'
            ' ON cancelamento_ano_aerodromo_brasil.aerodromo=aerodromos_mais_usados_brasil.aerodromo'
            ' ORDER BY ano_voo, total DESC'
        )
        codigo, mensagem, df = self.executar_query(query)

        if codigo == 0:
            return df, query
        
        else:
            raise Exception(mensagem)
