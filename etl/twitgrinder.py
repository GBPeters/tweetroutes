__author__ = 'gijspeters'

import datetime
import dbconnect
import ppygis
import sys

import twitscraper
from db import dbtables

LINKS=4.73
BOVEN=52.43
RECHTS=5.015
ONDER=52.285

class TweetGrinder:

    def __init__(self):
        dbtables.SCRAPED_TWEETS.createTable()
        self.kursor = dbconnect.Verbinding().kursor

    def verwerkTweets(self, data, naam):
        ntweets = len(data)
        sql = ''
        if ntweets > 50:
            ngeotagged = 0
            ngeotaggedA = 0
            locatedTweets = []
            print ""
            i = 0
            for tweet in data:
                i = i + 1
                if tweet['coordinates'] != None:
                    ngeotagged = ngeotagged + 1
                    coordinates = tweet['coordinates']['coordinates']
                    lat = float(coordinates[1])
                    lon = float(coordinates[0])
                    if (lat >= ONDER and lat <= BOVEN) and (lon >= LINKS and lon <= RECHTS):
                        ngeotaggedA = ngeotaggedA + 1
                        datumtijd = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                        locatedTweets.append((tweet['id_str'], naam, datumtijd, ppygis.Point(lat, lon).getquoted(), lat, lon, datumtijd.date()))
            print "Tweet %d / %d, geotagged: %d, in Amsterdam: %d" % (i, ntweets, ngeotagged, ngeotaggedA)
            if (ngeotaggedA > 5 and float(ngeotaggedA/ngeotagged) < 0.2):
                for tweet in locatedTweets:
                    sql = "INSERT INTO scraped_tweets (tweet_id, tweet_name, tijddatum, the_geom, lat, lon, datum) VALUES ('%s', '%s', '%s', %s, %f, %f, '%s')" % tweet
                    self.kursor.execute(sql)
                    sql = "UPDATE users SET processed=true WHERE name='%s'" % naam
                    self.kursor.execute(sql)
            else:
                sql = "UPDATE users SET useless=true WHERE name='%s'" % naam
                self.kursor.execute(sql)
        else:
            sql = "UPDATE users SET useless=true WHERE name='%s'" % naam
            self.kursor.execute(sql)
        self.kursor.connection.commit()





    def klaar(self):
        self.kursor.close()

def FetchUsefulTweets(namen):
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT min(tweet_id), max(tweet_id) FROM tweets_amsterdam"
    kursor.execute(sql)
    (firstId, lastId) = kursor.fetchone()
    scraper = twitscraper.TimelineScraper(int(firstId), int(lastId))
    grinder = TweetGrinder()
    for naam in namen:
        scraper.pushOpdracht(naam[0], grinder.verwerkTweets)
    kursor.close()
    scraper.scrape()
    grinder.klaar()

def StartFetching():
    kursor = dbconnect.Verbinding().kursor
    sql = "UPDATE users SET useless=false, processed=false"
    kursor.execute(sql)
    sql = "UPDATE users SET useless=true WHERE days>15 OR tweets<2"
    kursor.execute(sql)
    kursor.connection.commit()
    sql = "SELECT name FROM users WHERE useless=false ORDER BY name"
    kursor.execute(sql)
    namen = kursor.fetchall()
    kursor.close()
    FetchUsefulTweets(namen)

def ContinueFetching():
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT name FROM users WHERE processed=false AND useless=false ORDER BY name"
    kursor.execute(sql)
    namen = kursor.fetchall()
    kursor.close()
    FetchUsefulTweets(namen)
