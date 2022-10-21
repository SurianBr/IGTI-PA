from multiprocessing.sharedctypes import Value
from airium import Airium

class Paginas():

    def __init__(self) -> None:
        
        # prepara variaveis
        self.home_page = None
        self.consulta = None
        self.metadados = None
    

    def get_home_page(self, dados):
        '''
            Monta home page
        '''

        if self.home_page is None:

            self.home_page = self.get_pagina_vazia()

            with self.home_page.body():
                self.home_page(self.get_barra_navegacao())
                self.home_page(self.get_body_home(dados))
                self.home_page(self.get_rodape())

        return self.home_page


    def get_consulta(self):
        '''
            Monta pagina de consulta
        '''

        if self.consulta is None:

            self.consulta = self.get_pagina_vazia()

            with self.consulta.body(style='height:100%'):
                self.consulta(self.get_barra_navegacao())
                self.consulta(self.get_body_consulta())
                self.consulta(self.get_rodape())

        return self.consulta


    def get_consulta_query(self, dados, query):
        '''
            Monta pagina com o resultado da query
        '''
        pagina = self.get_pagina_vazia()
        with pagina.body(style='height:100%'):
            pagina(self.get_barra_navegacao())
            pagina(self.get_body_consulta_query(dados, query))
            pagina(self.get_rodape())

        return pagina


    def get_cancelamentos(self, dados):
        '''
            Monta pagina com o resultado da query
        '''
        pagina = self.get_pagina_vazia()
        with pagina.body(style='height:100%'):
            pagina(self.get_barra_navegacao())
            pagina(self.get_body_cancelamentos(dados))
            pagina(self.get_rodape())

        return pagina


    def get_metadados(self):
        
        if self.metadados is None:

            self.metadados = self.get_pagina_vazia()

            with self.metadados.body():
                self.metadados(self.get_barra_navegacao())
                self.metadados(self.get_body_metadados())
                self.metadados(self.get_rodape())

        return self.metadados


    def get_pagina_vazia(self):
        a = Airium()

        a('<!DOCTYPE html>')
        with a.html(lang='en'):
            with a.head():
                a.title(_t='Voo Regular Ativo')
                a.meta(charset='UTF-8')
                a.meta(content='width=device-width, width=device-width, initial-scale=1', name='viewport')
                a.link(href='https://www.w3schools.com/w3css/4/w3.css', rel='stylesheet')
                a.link(href='https://fonts.googleapis.com/css?family=Lato', rel='stylesheet')
                a.link(href='https://fonts.googleapis.com/css?family=Montserrat', rel='stylesheet')
                a.link(href='https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css', rel='stylesheet')
                a.script(src="https://cdn.plot.ly/plotly-2.14.0.min.js")
                with a.style():
                    a('body,h1,h2,h3,h4,h5,h6 {font-family: "Lato", sans-serif}\n.w3-bar,h1,button {font-family: "Montserrat", sans-serif}\n.fa-anchor,.fa-coffee {font-size:200px}')
            a.body()

        return a

    def get_barra_navegacao(self):
        a = None
        a = Airium()

        a('<!-- Navbar -->')
        with a.div(klass='w3-top'):
            with a.div(klass='w3-bar w3-blue w3-card w3-left-align w3-large'):
                with a.a(klass='w3-bar-item w3-button w3-hide-medium w3-hide-large w3-right w3-padding-large w3-hover-white w3-large w3-blue', href='javascript:void(0);', onclick='myFunction()', title='Toggle Navigation Menu'):
                    a.i(klass='fa fa-bars')
                a.a(klass='w3-bar-item w3-button w3-padding-large w3-white', href='/', _t='Home')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='consulta', _t='Consulta')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='cancelamentos', _t='Cancelamentos')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='metadados', _t='Metadados')
            a('<!-- Navbar on small screens -->')
            with a.div(klass='w3-bar-block w3-white w3-hide w3-hide-large w3-hide-medium w3-large', id='navDemo'):
                a.a(klass='w3-bar-item w3-button w3-padding-large w3-white', href='/', _t='Home')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='consulta', _t='Consulta')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='cancelamentos', _t='Cancelamentos')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='metadados', _t='Metadados')

        return a


    def get_body_home(self, dados):
        '''
            Monta a pagina home

            Parametros:
                dados (list: Dataframe Spark): Lista com os Dataframes que 
                iram ser usados para montar a home page
        '''
        a = Airium()

        # Busca scripts dos graficos
        script_voos_ano = open('js/home_page/grafico_voos_ano.js', 'r', encoding='utf-8')
        script_voos_ano_string = script_voos_ano.read()
        script_voos_ano.close()

        # Prepara dados
        # Total de voos
        total_voos = dados[0].toPandas().to_dict(orient='list')['quantidade_voos'][0]
        total_voos = format(total_voos, ',')

        # voos por ano
        dados_voos_ano_dict = dados[1].toPandas().to_dict(orient='list')

        eixo_x = str(dados_voos_ano_dict['ano_voo']).replace('\'', '')
        eixo_y = str(dados_voos_ano_dict['quantidade_voos']).replace('\'', '')

        # top 20 rotas
        top_20_rotas = dados[2].toPandas().to_dict(orient='list')

        # top 20 aerodromos
        top_20_aerodromos = dados[3].toPandas().to_dict(orient='list')

        # Coloca os dados no script
        script_voos_ano_string = script_voos_ano_string.replace(
            '|x_total_voos|',
            eixo_x
        )
        script_voos_ano_string = script_voos_ano_string.replace(
            '|y_total_voos|',
            eixo_y
        )
        
        a('<!-- Header -->')
        with a.header(klass='w3-container w3-blue w3-center w3-padding-32'):
            a.h1(klass='w3-margin w3-jumbo', _t='Voo Regular Ativo')

            with a.p(klass='w3-xlarge'):
                a(
                    'Informações sobre voos de empresas de serviços de transporte aéreo '
                    'realizados no espaço aéreo brasileiro.'
                )

        a('<!-- Total Voos -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center'):
                with a.p(klass='w3-large'):
                    a('Quantidade de voos na base de dados:')
            with a.div(klass='w3-container w3-center'):
                with a.p(klass='w3-xxxlarge'):
                    a(total_voos)

        a('<!-- Voos por ano -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Voos realizados por ano:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='voos_ano', klass='w3-auto'):
                    with a.script():
                        a(script_voos_ano_string)
            a.div(klass='w3-container w3-padding w3-light-grey')

        a('<!-- Top 20 Rotas -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Top 20 Rotas')
            with a.div(klass='w3-responsive'):
                with a.table(klass='w3-auto w3-table-all'):
                    with a.tr(klass="w3-blue"):  # cabecalho tabela
                        with a.th():
                            a('Aeródromo de Origem')
                        with a.th():
                            a('Cidade de Origem')
                        with a.th():
                            a('Aerodromo de Destino')
                        with a.th():
                            a('Cidade de Destino')
                        with a.th():
                            a('Número de Voos')
                
                    for i in range(20):  # Gera linhas das tabela
                        with a.tr():
                            with a.th():
                                a(top_20_rotas['aerodromo_origem'][i])
                            with a.th():
                                a(top_20_rotas['municipio_origem'][i])
                            with a.th():
                                a(top_20_rotas['aerodromo_destino'][i])
                            with a.th():
                                a(top_20_rotas['municipio_destino'][i])
                            with a.th():
                                a(format(top_20_rotas['quantidade_voos'][i], ','))

        a('<!-- Top 20 Aerodromos -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Top 20 Aeródromo')
            with a.div(klass='w3-responsive'):
                with a.table(klass='w3-auto w3-table-all'):
                    with a.tr(klass="w3-blue"):  # cabecalho tabela
                        with a.th():
                            a('Aeródromo')
                        with a.th():
                            a('Cidade')
                        with a.th():
                            a('Decolagens')
                        with a.th():
                            a('Aterrissagens')
                        with a.th():
                            a('Total')
                
                    for i in range(20):  # Gera linhas das tabela
                        with a.tr():
                            with a.th():
                                a(top_20_aerodromos['nome'][i])
                            with a.th():
                                a(top_20_aerodromos['municipio'][i])
                            with a.th():
                                a(format(top_20_aerodromos['decolagens'][i], ','))
                            with a.th():
                                a(format(top_20_aerodromos['aterrisagens'][i], ','))
                            with a.th():
                                a(format(top_20_aerodromos['total'][i], ','))
                        
        return a


    def get_body_consulta(self):
        a = Airium()

        # Busca scripts dos graficos
        script_consulta = open('js/home_page/consulta.js', 'r', encoding='utf-8')
        script_consulta_string = script_consulta.read()
        script_consulta.close()

        a('<!-- Header -->')
        with a.header(klass='w3-container w3-blue w3-center w3-padding-32'):
            a.h1(klass='w3-margin w3-jumbo', _t='Consulta')

            with a.p(klass='w3-xlarge'):
                a(
                    'Cosulte os dados disponívies escrevendo queries em SQL.'
                )
                a.br()
                a(
                    'Para informações sobre as tabelas disponíveis e seus campos, vá para '
                )
                with a.a(
                    href='metadados',
                    target="_blank"
                    ):
                    a('metadados')
                a(
                    '.'
                )

        a('<!-- Consulta -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            a.div(klass='w3-container w3-quarter')
            with a.div(klass='w3-container w3-center w3-half'):
                with a.p(klass='w3-large'):
                    a('Query')
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext")
                    with a.div(id='editor', style="height:500px"):
                    
                        a.script(
                        src="https://cdnjs.cloudflare.com/ajax/libs/ace/1.11.2/ace.js",
                        integrity="sha512-AhCq6G80Ge/e6Pl3QTNGI2Je+6ixVVDmmE4Nui8/dHRBKxMUvjJxn6CYEcMQdTSxHreC3USOxTDrvUPLtN5J7w==",
                        crossorigin="anonymous",
                        referrerpolicy="no-referrer"
                        )
                    a.script(
                        src="https://cdnjs.cloudflare.com/ajax/libs/ace/1.11.2/theme-monokai.min.js",
                        integrity="sha512-vH1p51CJtqdqWMpL32h5B9600achcN1XeTfd31hEcrCcCb5PCljIu7NQppgdNtdsayRQTnKmyf94s6HYiGQ9BA==",
                        crossorigin="anonymous",
                        referrerpolicy="no-referrer"
                    )
                    a.script(
                        src="https://cdnjs.cloudflare.com/ajax/libs/ace/1.11.2/snippets/mysql.min.js",
                        integrity="sha512-eASrazWxTooSzJmaO4rySNPxEGbhWeaFpzES97jflaFDoNjegixdAPt+yx0WBm8n5SoagkWNYv+ol0ou52MQtg==",
                        crossorigin="anonymous",
                        referrerpolicy="no-referrer"
                    )
                    
                    with a.script():
                        a(script_consulta_string)

                    a.br()
                    a.input(type='submit', value='Consultar')
            a.div(klass='w3-container w3-quarter')

        return a


    def get_body_consulta_query(self, dados, query):
        a = Airium()
        
        # Prepara dados
        dados_dict = dados.toPandas().to_dict(orient="list")
        chaves = list(dados_dict.keys())

        a('<!-- Header -->')
        with a.header(klass='w3-container w3-blue w3-center w3-padding-32'):
            a.h1(klass='w3-margin w3-jumbo', _t='Resultado da Query')


        a('<!-- Query -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.b():
                a('Query Processada')

            a('<br>')

            with a.div(klass='w3-responsive'):
                a(query.replace('|', '<br>'))

        a('<!-- Tabela de resultado -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-responsive'):
                with a.table(klass='w3-auto w3-table-all'):
                    with a.tr(klass="w3-blue"):  # cabecalho tabela
                        for chave in chaves:
                            with a.th():
                                a('{0}'.format(chave))
                
                    for i in range(len(dados_dict[chaves[0]])):  # Gera linhas das tabela
                        with a.tr():
                            for chave in chaves:
                                with a.th():
                                    a(dados_dict.get(chave)[i])


        a('<!-- Tabela de resultado -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.form(id='form_query', action='/download-query', method='get'):
                a.input('hidden', name="query", type="text", id="editortext", value=query)
                a.input(type='submit', value='Download Dados')

        return a


    def get_body_cancelamentos(self, dados):
        '''
            Monta a pagina home

            Parametros:
                dados (list: Dataframe Spark): Lista com os Dataframes que 
                iram ser usados para montar a home page
        '''
        a = Airium()

        # Busca scripts dos graficos
        script_cancelamento_ano = open('js/home_page/grafico_cancelamentos_ano.js', 'r', encoding='utf-8')
        script_cancelamento_string = script_cancelamento_ano.read()
        script_cancelamento_ano.close()

        script_cancelamento_ano_boxplot = open('js/home_page/grafico_cancelamentos_ano_boxplot.js', 'r', encoding='utf-8')
        script_cancelamento_string_boxplot = script_cancelamento_ano_boxplot.read()
        script_cancelamento_ano_boxplot.close()

        script_cancelamento_ano_empresa = open('js/home_page/grafico_cancelamentos_ano_empresa.js', 'r', encoding='utf-8')
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa.read()
        script_cancelamento_ano_empresa.close()

        script_cancelamento_ano_empresa_boxplot = open('js/home_page/grafico_cancelamentos_ano_empresa_boxplot.js', 'r', encoding='utf-8')
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot.read()
        script_cancelamento_ano_empresa_boxplot.close()

        script_cancelamentos_ano_aerodromo = open('js/home_page/grafico_cancelamentos_ano_aerodromo.js', 'r', encoding='utf-8')
        script_cancelamentos_ano_aerodromo_string = script_cancelamentos_ano_aerodromo.read()
        script_cancelamentos_ano_aerodromo.close()

        script_cancelamentos_ano_aerodromo_boxplot = open('js/home_page/grafico_cancelamentos_ano_aerodromos_boxplot.js', 'r', encoding='utf-8')
        script_cancelamentos_ano_aerodromo_boxplot_string = script_cancelamentos_ano_aerodromo_boxplot.read()
        script_cancelamentos_ano_aerodromo_boxplot.close()

        # Prepara dados
        # voos cancelados por ano
        dados_cancelamento_ano_dict = dados[0]['df'].toPandas().to_dict(orient='list')

        eixo_x = str(dados_cancelamento_ano_dict['ano_voo']).replace('\'', '')
        eixo_y = str(dados_cancelamento_ano_dict['porcentage_cancelado']).replace('\'', '')

        # voos cancelados por ano e empresa
        dados_cancelamento_ano__empresa = dados[1]['df'].toPandas()
        cancelamento_ano_empresa_tam_dict = dados_cancelamento_ano__empresa[dados_cancelamento_ano__empresa['icao_empresa_aerea'] == 'TAM'].to_dict(orient='list')
        cancelamento_ano_empresa_gol_dict = dados_cancelamento_ano__empresa[dados_cancelamento_ano__empresa['icao_empresa_aerea'] == 'GLO'].to_dict(orient='list')
        cancelamento_ano_empresa_azul_dict = dados_cancelamento_ano__empresa[dados_cancelamento_ano__empresa['icao_empresa_aerea'] == 'AZU'].to_dict(orient='list')

        tam_x = str(cancelamento_ano_empresa_tam_dict['ano_voo']).replace('\'', '')
        tam_y = str(cancelamento_ano_empresa_tam_dict['porcentage_cancelado']).replace('\'', '')

        gol_x = str(cancelamento_ano_empresa_gol_dict['ano_voo']).replace('\'', '')
        gol_y = str(cancelamento_ano_empresa_gol_dict['porcentage_cancelado']).replace('\'', '')

        azul_x = str(cancelamento_ano_empresa_azul_dict['ano_voo']).replace('\'', '')
        azul_y = str(cancelamento_ano_empresa_azul_dict['porcentage_cancelado']).replace('\'', '')

        # voos cancelados por ano e aerodromo
        cancelamento_ano_aerodromo_dict = dados[2]['df'].toPandas().to_dict(orient='records')

        cancelamento_ano_aerodromo_eixo_x = []
        cancelamento_ano_aerodromo_eixos_y= {}
        for linha in cancelamento_ano_aerodromo_dict:

            if linha['ano_voo'] not in cancelamento_ano_aerodromo_eixo_x:
                cancelamento_ano_aerodromo_eixo_x.append(linha['ano_voo'])

            if cancelamento_ano_aerodromo_eixos_y.get(linha['aerodromo'], None) is None:
                cancelamento_ano_aerodromo_eixos_y[linha['aerodromo']] = [linha['porcentage_cancelado']]
            else:
                cancelamento_ano_aerodromo_eixos_y[linha['aerodromo']].append(linha['porcentage_cancelado'])

        cancelamento_ano_aerodromo_eixo_x = str(cancelamento_ano_aerodromo_eixo_x).replace('\'', '')

        # Coloca os dados no script
        # voos cancelados por ano
        script_cancelamento_string = script_cancelamento_string.replace(
            '|x_cancelamentos|',
            eixo_x
        )
        script_cancelamento_string = script_cancelamento_string.replace(
            '|y_cancelamentos|',
            eixo_y
        )

        # voos cancelados por ano box plot
        script_cancelamento_string_boxplot = script_cancelamento_string_boxplot.replace(
            '|x_cancelamentos|',
            eixo_x
        )
        script_cancelamento_string_boxplot = script_cancelamento_string_boxplot.replace(
            '|y_cancelamentos|',
            eixo_y
        )

        # voos cancelados por ano e empresa
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa_string.replace(
            '|tam_x|',
            tam_x
        )
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa_string.replace(
            '|tam_y|',
            tam_y
        )
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa_string.replace(
            '|gol_x|',
            gol_x
        )
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa_string.replace(
            '|gol_y|',
            gol_y
        )
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa_string.replace(
            '|azul_x|',
            azul_x
        )
        script_cancelamento_ano_empresa_string = script_cancelamento_ano_empresa_string.replace(
            '|azul_y|',
            azul_y
        )

        # voos cancelados por ano e empresa boxplot
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot_string.replace(
            '|tam_x|',
            tam_x
        )
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot_string.replace(
            '|tam_y|',
            tam_y
        )
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot_string.replace(
            '|gol_x|',
            gol_x
        )
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot_string.replace(
            '|gol_y|',
            gol_y
        )
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot_string.replace(
            '|azul_x|',
            azul_x
        )
        script_cancelamento_ano_empresa_boxplot_string = script_cancelamento_ano_empresa_boxplot_string.replace(
            '|azul_y|',
            azul_y
        )

        # voos cancelados por ano e aerodromo e boxplot
        i = 1
        for aerodromo in cancelamento_ano_aerodromo_eixos_y:
            numero_aerodromo = 'aerodromo_{0}'.format(i)
            eixo_y = str(cancelamento_ano_aerodromo_eixos_y[aerodromo]).replace('\'', '')

            script_cancelamentos_ano_aerodromo_string = script_cancelamentos_ano_aerodromo_string.replace(
                '|{0}_x|'.format(numero_aerodromo),
                cancelamento_ano_aerodromo_eixo_x
            )
            script_cancelamentos_ano_aerodromo_string = script_cancelamentos_ano_aerodromo_string.replace(
                '|{0}_y|'.format(numero_aerodromo),
                eixo_y
            )
            script_cancelamentos_ano_aerodromo_string = script_cancelamentos_ano_aerodromo_string.replace(
                '|{0}|'.format(numero_aerodromo),
                aerodromo
            )

            script_cancelamentos_ano_aerodromo_boxplot_string = script_cancelamentos_ano_aerodromo_boxplot_string.replace(
                '|{0}_x|'.format(numero_aerodromo),
                cancelamento_ano_aerodromo_eixo_x
            )
            script_cancelamentos_ano_aerodromo_boxplot_string = script_cancelamentos_ano_aerodromo_boxplot_string.replace(
                '|{0}_y|'.format(numero_aerodromo),
                eixo_y
            )
            script_cancelamentos_ano_aerodromo_boxplot_string = script_cancelamentos_ano_aerodromo_boxplot_string.replace(
                '|{0}|'.format(numero_aerodromo),
                aerodromo
            )
            i += 1

        
        a('<!-- Header -->')
        with a.header(klass='w3-container w3-blue w3-center w3-padding-32'):
            a.h1(klass='w3-margin w3-jumbo', _t='Cancelamentos')

            with a.p(klass='w3-xlarge'):
                a(
                    'Informações sobre cancelamentos de voos'
                )


        a('<!-- Cancelamento por ano -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Percentual de cancelamentos de voos por ano:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='cancelamentos_ano', klass='w3-auto'):
                    with a.script():
                        a(script_cancelamento_string)
            with a.div(klass='w3-container w3-padding'):
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext", value=dados[0]['query'])
                    a.input(type='submit', value='Visualisar Dados')


        a('<!-- Cancelamento por ano boxplot -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Boxplot dos cancelamentos de voos por ano:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='cancelamentos_ano_boxplot', klass='w3-auto'):
                    with a.script():
                        a(script_cancelamento_string_boxplot)
            with a.div(klass='w3-container w3-padding w3-light-grey'):
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext", value=dados[0]['query'])
                    a.input(type='submit', value='Visualisar Dados')


        a('<!-- Cancelamento por ano e empresa -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Cancelamentos de voos por ano e empresas TOP 3:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='cancelamentos_ano_empresa', klass='w3-auto'):
                    with a.script():
                        a(script_cancelamento_ano_empresa_string)
            with a.div(klass='w3-container w3-padding'):
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext", value=dados[1]['query'])
                    a.input(type='submit', value='Visualisar Dados')


        a('<!-- boxplot Cancelamento por ano e empresa -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Boxplot dos cancelamentos de voos por ano e empresas TOP 3:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='cancelamentos_ano_empresa_boxplot', klass='w3-auto'):
                    with a.script():
                        a(script_cancelamento_ano_empresa_boxplot_string)
            with a.div(klass='w3-container w3-padding w3-light-grey'):
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext", value=dados[1]['query'])
                    a.input(type='submit', value='Visualisar Dados')


        a('<!-- Cancelamento por ano e aerodromo -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Cancelamentos de voos por ano e aeródromo TOP 20:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='cancelamentos_ano_aerodromo', klass='w3-auto'):
                    with a.script():
                        a(script_cancelamentos_ano_aerodromo_string)
            with a.div(klass='w3-container w3-padding'):
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext", value=dados[2]['query'])
                    a.input(type='submit', value='Visualisar Dados')


        a('<!-- Cancelamento por ano e aerodromo -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Boxplot dos cancelamentos de voos por ano e aeródromo TOP 20:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='cancelamentos_ano_aerodromo_boxplot', klass='w3-auto'):
                    with a.script():
                        a(script_cancelamentos_ano_aerodromo_boxplot_string)
            with a.div(klass='w3-container w3-padding w3-light-grey'):
                with a.form(id='form_query', action='/consultar', method='get', target='_blank'):
                    a.input('hidden', name="query", type="text", id="editortext", value=dados[2]['query'])
                    a.input(type='submit', value='Visualisar Dados')
                        
        return a


    def get_body_metadados(self):
        a = Airium()

        a('<!-- Header -->')
        with a.header(klass='w3-container w3-blue w3-center w3-padding-32'):
            a.h1(klass='w3-margin w3-jumbo', _t='Metados')

            with a.p(klass='w3-xlarge'):
                a(
                    'Informações sobre as tabelas diponiveis e descrições de suas colunas.'
                )

        a('<!-- VRA -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('VRA - Voo Regular Ativo')
                with a.p():
                    a(
                        'O Voo Regular Ativo – VRA é uma base de dados composta por informações de voos de empresas de transporte aéreo '
                        'regular que apresenta alterações de voos (atrasos, antecipações e cancelamentos), bem como horários em que os voos ocorreram. '
                        'Por meio desta base de dados, podem ser obtidos os percentuais de atrasos e cancelamentos. O mês publicado se refere às etapas '
                        'cujas decolagens eram previstas para o mês em questão ou cujas decolagens, em caso de etapa não prevista, foram realizadas no mês em questão. '
                        'O VRA é formado pela junção das informações, fornecidas pelas empresas de transporte aéreo, relativas aos voos planejados e aos voos realizados. '
                    )
                with a.p():
                    with a.b():
                        a('Nome da tabela para consultas: ')
                    a('vra')
                with a.p():
                    with a.b():
                        a('Fonte: ')
                    with a.a(
                        href='https://www.anac.gov.br/acesso-a-informacao/dados-abertos/areas-de-atuacao/voos-e-operacoes-aereas/voo-regular-ativo-vra',
                        target="_blank"
                        ):
                        a('ANAC')

            with a.div(klass='w3-responsive'):
                with a.table(klass='w3-auto w3-table-all'):
                    with a.tr(klass="w3-blue"):  # cabecalho tabela
                        with a.th():
                            a('Campo')
                        with a.th():
                            a('Descrição')

                    with a.tr(): 
                        with a.th():
                            a('icao_empresa_aerea')
                        with a.th():
                            a('Sigla/Designador ICAO Empresa Aérea')
                    with a.tr(): 
                        with a.th():
                            a('numero_voo')
                        with a.th():
                            a('Numeração do voo')
                    with a.tr(): 
                        with a.th():
                            a('codigo_autorizacao')
                        with a.th():
                            a('Caractere usado para identificar o Dígito Identificador (DI) para cada etapa de voo')
                    with a.tr(): 
                        with a.th():
                            a('codigo_tipo_linha')
                        with a.th():
                            a('Caractere usado para identificar o Tipo de Linha realizada para cada etapa de voo')
                    with a.tr(): 
                        with a.th():
                            a('icao_aerodromo_origem')
                        with a.th():
                            a('Sigla/Designador ICAO aeródromo de Origem')
                    with a.tr(): 
                        with a.th():
                            a('icao_aerodromo_destino')
                        with a.th():
                            a('Sigla/Designador ICAO aeródromo de Destino')
                    with a.tr(): 
                        with a.th():
                            a('data_partida_prevista')
                        with a.th():
                            a('Data e horário da partida prevista informada pela empresa aérea, em horário de Brasília')
                    with a.tr(): 
                        with a.th():
                            a('data_partida_real')
                        with a.th():
                            a('Data e horário da partida realizada informada pela empresa aérea, em horário de Brasília')
                    with a.tr(): 
                        with a.th():
                            a('data_chegada_prevista')
                        with a.th():
                            a('Data e horário da chegada prevista informada pela empresa aérea, em horário de Brasília')
                    with a.tr(): 
                        with a.th():
                            a('data_chegada_real')
                        with a.th():
                            a('Data e horário da chegada realizada, informada pela empresa aérea, em horário de Brasília')
                    with a.tr(): 
                        with a.th():
                            a('situacao_voo')
                        with a.th():
                            a('Situação do voo: realizado, cancelado ou não informado​')
                    with a.tr(): 
                        with a.th():
                            a('codigo_justificativa')
                        with a.th():
                            a('Código de justificação da situação do voo​')
                    with a.tr(): 
                        with a.th():
                            a('ano_voo')
                        with a.th():
                            a('Ano que ocorreu o voo​')
                    with a.tr(): 
                        with a.th():
                            a('mes_voo')
                        with a.th():
                            a('Mês que ocorreu o voo​')
                    with a.tr(): 
                        with a.th():
                            a('arquivo_origem')
                        with a.th():
                            a('Arquivo de origem do dado​')

            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p():
                    with a.b():
                        a('Formato do arquivo: ')
                    a('Parquet')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.form(action='/download-vra', method='get'):
                    a.input(type='submit', value='Download VRA')


        a('<!-- Aerodromos -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Aeródromos')
                with a.p():
                    a(
                        'Dados sobre os aerodromos do Brasil e do Mundo.'
                    )
                with a.p():
                    with a.b():
                        a('Nome da tabela para consultas: ')
                    a('aerodromos')
                with a.p():
                    with a.b():
                        a('Fonte: ')
                    with a.a(
                        href='https://ourairports.com/data/',
                        target="_blank"
                        ):
                        a('OurAirports')
            with a.div(klass='w3-responsive'):
                with a.table(klass='w3-auto w3-table-all'):
                    with a.tr(klass="w3-blue"):  # cabecalho tabela
                        with a.th():
                            a('Campo')
                        with a.th():
                            a('Descrição')

                    with a.tr(): 
                        with a.th():
                            a('id')
                        with a.th():
                            a('Código aeródromo do site OurAirports')
                    with a.tr(): 
                        with a.th():
                            a('icao')
                        with a.th():
                            a('Sigla/Designador ICAO aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('nome')
                        with a.th():
                            a('Nome aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('latitude_graus')
                        with a.th():
                            a('Latitude em graus aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('longitude_graus')
                        with a.th():
                            a('Longitude em graus aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('elevacao_pes')
                        with a.th():
                            a('Elevação em pés aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('continente')
                        with a.th():
                            a(
                                'Código do continente onde está localizado o aeródromo. <br>'
                                ' Valores: "AF" Africa, "AN" Antártida, "AS" Asia, "EU" Europa, '
                                '"NA" América do Norte, "SA" América do Sul e "OC" Oceania'
                            )
                    with a.tr(): 
                        with a.th():
                            a('regiao_iso')
                        with a.th():
                            a('Código ISO 3166-2 da província/estado do pais onde está localizado o aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('municipio')
                        with a.th():
                            a('municipio onde está localizado o aeródromo')
                    with a.tr(): 
                        with a.th():
                            a('iata')
                        with a.th():
                            a('Sigla/Designador IATA aeródromo.')

            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p():
                    with a.b():
                        a('Formato do arquivo: ')
                    a('Parquet')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.form(action='/download-aerodromos', method='get'):
                    a.input(type='submit', value='Download Aerodromos')

        a('<!-- Empreasas -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Empresas Aéreas')
                with a.p():
                    a(
                        'Dados sobre as empresas apereas'
                    )
                with a.p():
                    with a.b():
                        a('Nome da tabela para consultas: ')
                    a('empreasas')
                with a.p():
                    with a.b():
                        a('Fonte: ')
                    with a.a(
                        href='https://openflights.org/data.html#airline',
                        target="_blank"
                        ):
                        a('Open Flights')
            with a.div(klass='w3-responsive'):
                with a.table(klass='w3-auto w3-table-all'):
                    with a.tr(klass="w3-blue"):  # cabecalho tabela
                        with a.th():
                            a('Campo')
                        with a.th():
                            a('Descrição')

                    with a.tr(): 
                        with a.th():
                            a('id')
                        with a.th():
                            a('Código aeródromo do site Open Flights')
                    with a.tr(): 
                        with a.th():
                            a('nome')
                        with a.th():
                            a('Nome da empresa aérea')
                    with a.tr(): 
                        with a.th():
                            a('apelido')
                        with a.th():
                            a('Apelido que a empresa aérea é conhecida')
                    with a.tr(): 
                        with a.th():
                            a('iata')
                        with a.th():
                            a('Sigla/Designador IATA da empresa aérea')
                    with a.tr(): 
                        with a.th():
                            a('icao')
                        with a.th():
                            a('Sigla/Designador ICAO da empresa aérea')
                    with a.tr(): 
                        with a.th():
                            a('indicativo_chamada')
                        with a.th():
                            a('Indicativo de chamada (Callsign) da empresa aérea')
                    with a.tr(): 
                        with a.th():
                            a('pais')
                        with a.th():
                            a('País de origem da empresa aérea')

            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p():
                    with a.b():
                        a('Formato do arquivo: ')
                    a('Parquet')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.form(action='/download-empresas', method='get'):
                    a.input(type='submit', value='Download Empresas')

        return a


    def get_rodape(self):
        a = Airium()
        
        a('<!-- Footer -->')
        with a.footer(klass='w3-container w3-blue w3-center w3-padding'):
            a.div(klass='w3-xlarge w3-padding')
            with a.p():
                a('Powered by')
                a.a(href='https://www.w3schools.com/w3css/default.asp', target='_blank', _t='w3.css')
        with a.script():
            a('// Used to toggle the menu on small screens when clicking on the menu button\nfunction myFunction() {\n  var x = document.getElementById("navDemo");\n  if (x.className.indexOf("w3-show") == -1) {\n    x.className += " w3-show";\n  } else { \n    x.className = x.className.replace(" w3-show", "");\n  }\n}')

        return a