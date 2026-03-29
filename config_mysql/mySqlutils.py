import mysql.connector

class MySQLUtils:
    '''
        Utilitários para operações comuns com MySQL, como conexão, criação de tabelas, inserção de dados, etc.
    '''

    def __init__(self):
        pass

    
    def conectar(self, host, user, password):
        '''
            Estabelece uma conexão com o servidor MySQL usando as credenciais fornecidas.
            Retorna o objeto de conexão.
        '''
        try:
            mydb = mysql.connector.connect(
                host=host,
                user=user,
                password=password
            )
            print("Conexão estabelecida com sucesso!")
            return mydb

        except mysql.connector.Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            raise e