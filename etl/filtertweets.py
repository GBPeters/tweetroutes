#__author__ = 'gijspeters'


import dbconnect

from db import dbtables
from etl import twitobjects
from misc import outputs

LINKS=4.73
BOVEN=52.43
RECHTS=5.015
ONDER=52.285

def filterTweets():
    kursor = dbconnect.Verbinding().kursor
    dbtables.TWEETS_AMSTERDAM.createTable()
    sql = "SELECT tweet_id, tweet_name, tweet_datetime, the_geom, latitude, longitude, date(tweet_datetime) " \
          "FROM tweetdata WHERE (latitude >= %f AND latitude <= %f AND longitude >= %f AND longitude <= %f)" % (ONDER, BOVEN, LINKS, RECHTS)
    print "Selecting..."
    kursor.execute(sql)
    totaal = kursor.rowcount
    rijen = kursor.fetchall()
    voortgang = outputs.VoortgangRegel(totaal, outputs.VoortgangRegel.MODUS_PROCENT, 'Voortgang')
    for rij in rijen:
        tw = twitobjects.createFromFetch(rij)
        sql = "INSERT INTO tweets_amsterdam (tweet_id, tweet_name, tijddatum, the_geom, lat, lon, datum) " \
              "VALUES ('%s', '%s', '%s', '%s', %f, %f, '%s')" % tw.getData()
        kursor.execute(sql)
        voortgang.plusEen()
    kursor.connection.commit()
    kursor.connection.close()



