import mysql.connector

class MySQLUtils:
    '''
        Utilitários para operações comuns com MySQL, como conexão, criação de tabelas, inserção de dados, etc.
    '''

    def __init__(self):
        pass

    
    def conectar(self, host, user, password, database=None):
        '''
            Estabelece uma conexão com o servidor MySQL usando as credenciais fornecidas.
            Retorna o objeto de conexão.
        '''
        try:
            myConexao = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            print("Conexão estabelecida com sucesso!")
            return myConexao

        except mysql.connector.Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            raise e
        

    def criar_database(self, myConexao, nome_database):
        '''Cria o banco de dados com o nome especificado.'''
        
        try:
            mycursor = myConexao.cursor()
        except mysql.connector.Error as e:
            print(f"Erro ao criar cursor: {e}")
            myConexao.close()

            raise e

        sql_query = f"CREATE DATABASE IF NOT EXISTS {nome_database}"

        try:
            mycursor.execute(sql_query)
        except mysql.connector.Error as e:
            print(f"Erro ao criar database {nome_database}: {e}")

            mycursor.close()
            myConexao.close()

            raise e

        mycursor.close()


    def  criar_tabela(self, myConexao, nome_database, nome_tabela, definicao_colunas):
        '''Cria uma tabela com a definição de colunas fornecida.'''
        
        try:
            mycursor = myConexao.cursor()
        except mysql.connector.Error as e:
            print(f"Erro ao criar cursor: {e}")
            myConexao.close()

            raise e

        sql_query = f"CREATE TABLE IF NOT EXISTS {nome_database}.{nome_tabela} ({definicao_colunas})"

        try:
            mycursor.execute(sql_query)
        except mysql.connector.Error as e:
            print(f"Erro ao criar tabela {nome_tabela}: {e}")

            mycursor.close()
            myConexao.close()

            raise e

        mycursor.close()