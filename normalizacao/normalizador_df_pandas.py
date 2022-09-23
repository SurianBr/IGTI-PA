import json
import pandas

class Normalizador_df_pandas():

    colunas_esperadas = None

    def __init__(self, caminho_colunas_esperadas) -> None:

        colunas_esperadas_arquivo = open(caminho_colunas_esperadas, 'r', encoding='utf-8')
        colunas_esperadas_string = colunas_esperadas_arquivo.read()

        self.colunas_esperadas = json.loads(colunas_esperadas_string)

        colunas_esperadas_arquivo.close()


    def normalizar(self, df_pandas):
        '''
            Normaliza as colunas de um DF pandas utilizando uma lista de colunas
            esperadas.

            Se a coluna esperada estiver no DF Pandas:
                Altera o nome da coluna para o nome final

            Se a coluna no DF Pandas não estiver na lista de colunas esperadas:
                Elimina a coluna do DF Pandas

            Se a coluna esperada não estiver no DF Pandas
                Para normalização

            Parametros:
                df_pandas (Dataframe Pandas): Dataframe Pandas do arquivo a
                ser normalizado.

            Retorno:
                Codigo processamento (int)
                Dataframe Pandas Normalizado ou Mensagem de erro

                Codigo processamento = 0:
                    normalizacao ok. Encontrou todas as colunas esperadas.
                    Retorna dataframe pandas normalizado

                Codigo processamento = -1:
                    Processamento com falha. Não encontrou alguna coluna
                    esperada no dataframe pandas.
                    Retorna uma string com o nome da coluna esperada que não foi
                    encontrada.
        '''
        # pega lista de colunas no df pandas
        colunas = df_pandas.columns.values.tolist()

        # Gera uma lista com o nome da coluna no df
        # pandas x o nome da coluna no arquivo final
        lista_colunas_de_para = []

        # Passa por cada coluna do df pandas
        for nome_coluna_arquivo in colunas:
            de_para_coluna = {}

            coluna_encontrada = False

            # Verifica se coluna do df pandas bate com alguma que está
            # na lista com os nomes esperados
            for coluna_esperada in self.colunas_esperadas['colunas']:
                for nome_coluna_possivel in coluna_esperada['nome_colunas_possiveis']:
                    if nome_coluna_arquivo == nome_coluna_possivel:
                        de_para_coluna['nome_final'] = (
                            coluna_esperada['nome_final']
                        )
                        de_para_coluna['nome_arquivo'] = nome_coluna_arquivo

                        lista_colunas_de_para.append(de_para_coluna)
                        coluna_encontrada = True
                        break

                # Pula para a proxima coluna do df pandas
                if coluna_encontrada is True:
                    break

        # Verifica se encontrou todas as colunas esperadas
        if len(lista_colunas_de_para) != len(self.colunas_esperadas['colunas']):

            # Busca as colunas não encontradas
            lista_nao_encontrado = []
            for coluna_esperada in self.colunas_esperadas['colunas']:
                encontrada = False
                for coluna_encontrada in lista_colunas_de_para:
                    if (coluna_encontrada['nome_final'] ==
                            coluna_esperada['nome_final']):
                        encontrada = True

                if encontrada is False:
                    lista_nao_encontrado.append(coluna_esperada['nome_final'])

            # Para processamento
            mensagem = (
                        'Coluna(s) {0} não encontrada(s)'
                    ).format(
                        lista_nao_encontrado
                    )
            return -1, mensagem

        # dicionario para o renomear as colunas e lista só com os nomes finais
        de_para_colunas = {}
        lista_colunas_final = []
        for coluna in lista_colunas_de_para:
            de_para_colunas[coluna['nome_arquivo']] = coluna['nome_final']
            lista_colunas_final.append(coluna['nome_final'])

        # Renomeia as colunas
        df_pandas_nome_colunas = df_pandas.rename(columns=de_para_colunas)

        # Seleciona apenas as colunas esperadas
        df_pandas_final = df_pandas_nome_colunas[lista_colunas_final]

        return 0, df_pandas_final