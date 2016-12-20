__author__ = 'gijspeters'

import dbconnect

from db import dbtables
from misc.outputs import VoortgangRegel


def completeUserTable():
    dbtables.USERS.createTable()
    print 'Selecting dates...'
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT tweet_name FROM tweets_amsterdam GROUP BY tweet_name, datum"
    kursor.execute(sql)
    datumnamen = kursor.fetchall()
    print 'Selecting tweets...'
    sql = "SELECT tweet_name FROM tweets_amsterdam"
    kursor.execute(sql)
    tweetnamen = kursor.fetchall()
    print 'Selecting users...'
    sql = "SELECT DISTINCT tweet_name FROM tweets_amsterdam"
    kursor.execute(sql)
    namen = kursor.fetchall()
    voortgang = VoortgangRegel(len(namen), VoortgangRegel.MODUS_NUM, 'Gebruikers verwerkt')
    for naam in namen:
        days = datumnamen.count(naam)
        tweets = tweetnamen.count(naam)
        sql = "INSERT INTO users (name, days, tweets) VALUES ('%s', %d, %d)" % (naam[0], days, tweets)
        kursor.execute(sql)
        voortgang.plusEen()
    kursor.connection.commit()
    kursor.close()
