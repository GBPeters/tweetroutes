__author__ = 'gijspeters'

from com.ziclix.python.sql import zxJDBC
from java.sql import DriverManager
from java.lang import Class
from org.postgresql.copy import CopyManager
from java.lang import String


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
        jdbc_url = 'jdbc:postgresql://%s:%s/%s' % (DB_HOST, DB_PORT, DB_NAME)
        self.konneksie = zxJDBC.connect(jdbc_url, DB_USER, DB_PASS, DB_DRIVER)
        self.kursor = self.konneksie.cursor()

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
        sql = "DROP TABLE IF EXISTS %s; %s" % (dbtable.getNaam(), dbtable.getSql())
        self.kursor.execute(sql)
        self.konneksie.commit()

class CopyVerbinding:

    def __init__(self):
        self.verbind()

    def verbind(self):
        Class.forName(DB_DRIVER)
        jdbc_url = 'jdbc:postgresql://%s:%s/%s' % (DB_HOST, DB_PORT, DB_NAME)
        self.konneksie = DriverManager.getConnection(jdbc_url, DB_USER, DB_PASS)
        self.konneksie.setAutoCommit(False)

    def getCopyStream(self, sql):
        cm = CopyManager(self.konneksie)
        return cm.copyIn(sql)

    def commit(self):
        self.konneksie.commit()

    def sluit(self):
        self.konneksie.close()