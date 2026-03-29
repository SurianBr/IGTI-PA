import mysql.connector

from config_mysql.mySqlutils import MySQLUtils


class CriarDatabase:
    def __init__(self, mydb, host, user, password, database_name):
        self.mydb = mydb
        self.host = host
        self.user = user
        self.password = password
        self.database_name = database_name

        self.mysql_utils = MySQLUtils()


    def criar_database(self):
        self.cria_database_sql()
        self.mydb.close()


    def conectar_mysql(self):
        '''Estabelece uma conexão com o servidor MySQL usando as credenciais fornecidas.'''
        return self.mysql_utils.conectar(self.host, self.user, self.password)
    

    def cria_database_sql(self):
        '''Cria o banco de dados com o nome especificado.'''
        print("Criando database...")
        
        try:
            mycursor = self.mydb.cursor()
        except mysql.connector.Error as e:
            print(f"Erro ao criar cursor: {e}")
            self.mydb.close()

            raise e

        sql_query = f"CREATE DATABASE IF NOT EXISTS {self.database_name}"

        try:
            mycursor.execute(sql_query)
        except mysql.connector.Error as e:
            print(f"Erro ao criar database: {e}")

            mycursor.close()
            self.mydb.close()

            raise e

        mycursor.close()
