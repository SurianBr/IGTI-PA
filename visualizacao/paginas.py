from airium import Airium

class Paginas():

    def __init__(self) -> None:
        
        # prepara variaveis
        self.home_page = None
    

    def get_home_page(self, dados):
        '''
            Monta home page
        '''

        if self.home_page is None:

            self.home_page = self.get_pagina_vazia()

            self.home_page.body(
                self.get_barra_navegacao(),
                self.get_body_home(dados),
                self.get_rodape()
            )

        return self.home_page


    def get_pagina_vazia(self):
        a = Airium()

        a('<!DOCTYPE html>')
        with a.html(lang='en'):
            with a.head():
                a.title(_t='Voo Regular Ativo')
                a.meta(charset='UTF-8')
                a.meta(content='width=device-width, initial-scale=1', name='viewport')
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
                a.a(klass='w3-bar-item w3-button w3-padding-large w3-white', href='#', _t='Home')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='#', _t='Link 1')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='#', _t='Link 2')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='#', _t='Link 3')
                a.a(klass='w3-bar-item w3-button w3-hide-small w3-padding-large w3-hover-white', href='#', _t='Link 4')
            a('<!-- Navbar on small screens -->')
            with a.div(klass='w3-bar-block w3-white w3-hide w3-hide-large w3-hide-medium w3-large', id='navDemo'):
                a.a(klass='w3-bar-item w3-button w3-padding-large', href='#', _t='Link 1')
                a.a(klass='w3-bar-item w3-button w3-padding-large', href='#', _t='Link 2')
                a.a(klass='w3-bar-item w3-button w3-padding-large', href='#', _t='Link 3')
                a.a(klass='w3-bar-item w3-button w3-padding-large', href='#', _t='Link 4')

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
        print(total_voos)
        total_voos = format(total_voos, ',')

        # voos por ano
        dados_voos_ano_dict = dados[1].toPandas().to_dict(orient='list')

        eixo_x = str(dados_voos_ano_dict['ano_voo']).replace('\'', '')
        eixo_y = str(dados_voos_ano_dict['quantidade_voos']).replace('\'', '')

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
                a('Informações sobre situação e horários, previstos e realizados, '
                    'de etapas de voos de empresas de serviços de transporte aéreo '
                    'realizados no espaço aéreo brasileiro.'
                )

        a('<!-- Total Voos -->')
        with a.div(klass='w3-container w3-center w3-padding'):
            with a.div(klass='w3-container w3-center'):
                with a.p(klass='w3-large'):
                    a('Quantidade de Voos na base de dados:')
            with a.div(klass='w3-container w3-center'):
                with a.p(klass='w3-xxxlarge'):
                    a(total_voos)

        a('<!-- Voos por ano -->')
        with a.div(klass='w3-container w3-center w3-padding w3-light-grey'):
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.p(klass='w3-large'):
                    a('Voos realizados por ano:')
            with a.div(klass='w3-container w3-center w3-padding-small'):
                with a.div(id='tester', klass='w3-auto'):
                    with a.script():
                        a(script_voos_ano_string)
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