#__author__ = 'gijspeters'

import psycopg2


DB_NAME='tweets'
DB_HOST='localhost'
DB_USER='postgres'
DB_PASS='postgres'
DB_PORT='5432'
DB_DRIVER='org.postgresql.Driver'

class Verbinding:

    def __init__(self):
        self.verbind()

    def verbind(self):
        self.konneksie = psycopg2.connect(database=DB_NAME, host=DB_HOST, user=DB_USER, password=DB_PASS, port=DB_PORT)
        self.kursor = self.konneksie.cursor()
        self.copyfrom = self.kursor.copy_from

    def exe(self, sql):
        self.kursor.execute(sql)

    def selectOne(self, sql):
        self.kursor.execute(sql)
        return self.kursor.fetchone()

    def selectAll(self, sql):
        self.kursor.execute(sql)
        return self.kursor.fetchall()

    def commit(self):
        self.konneksie.commit()

    def sluit(self):
        self.konneksie.close()

    def createTable(self, dbtable):
        sql = "DROP TABLE IF EXISTS %s; " % (dbtable.naam) + dbtable.sql
        self.kursor.execute(sql)
        self.konneksie.commit()

