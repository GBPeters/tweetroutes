import os
import sys

__author__ = 'gijspeters'

# This class uses the Twython interface; you can get it from pip
from twython import Twython, TwythonError
import time

# You need one or more Twitter API Keys for this to work, fill in the keys and secrets in the lists below
API_KEY = []
API_SECRET = []

# A class providing methods for verbose display
class ScrapeMonitor:

    def __init__(self, totaal, firstId):
        self.totaal = totaal
        self.firstId = firstId
        self.nieuweMonitor()
        self.huidig = -1
        self.huidigeNaam = ''

    def nieuweMonitor(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print "TimelineScraper"
        print "Users processed: 0 / %d" % self.totaal
        print "Current user:"
        print "No update."

    def printSuccess(self, naam, pagina, aantal, oudsteId, n, r):
        self.updateRegel(naam, n, r)
        sys.stdout.write("Page %d, count:%d, oldest ID: %d" % (pagina, aantal, oudsteId))
        sys.stdout.flush()

    def printError(self, naam, message, n, r):
        self.updateRegel(naam, n, r)
        sys.stdout.write(message)
        sys.stdout.flush()

    def printWachten(self, seconden, n, r):
        self.updateRegel(self.huidigeNaam, n, r)
        sys.stdout.write("x-rate limit reached, resume scraping on %s" % time.strftime("%H:%M:%S", time.localtime(seconden)))
        sys.stdout.flush()

    def updateRegel(self, naam, n, r):
        if naam != self.huidigeNaam:
            self.huidigeNaam = naam
            self.huidig = self.huidig + 1
        os.system('cls' if os.name == 'nt' else 'clear')
        sys.stdout.write("TimelineScraper, Appnr: %d, %d requests left \n" % (n, r))
        sys.stdout.write("Users processed: %d / %d \n" % (self.huidig, self.totaal))
        sys.stdout.write("Current user: %s \n" % naam)

# The timeline scraper class.
# This class can be subclassed, make sure to call TimelineScraper.__init__(self, firstId, lastId)
# It requires as timeframe constructed by firstId and lastId, twitter ids are assigned chronologically.
# In this, firstId is the most recent id, and lastId is the oldest id.
# lastId is zero by default, it will download a complete timeline (with a maximum of 1200 tweets, a limit imposed by Twitter)

class TimelineScraper (object):

    # Class init
    def __init__(self, firstId, lastId=0):
        self.twitters = []
        for i in range(len(API_KEY)):
            token = Twython(API_KEY[i], API_SECRET[i], oauth_version=2).obtain_access_token()
            self.twitters.append(Twython(API_KEY[i], access_token=token))
        self.wachtrij = []
        self.FIRST_ID = firstId
        self.LAST_ID = lastId

    # Before scraping, add twitter user names by calling this method. 'naam' is the twitter name string, 'callback' the callback
    # method that is called after timeline collection has been completed.
    # The callback method needs two parameters; the first one is the name, the first being the result data list, the second being the user name.
    def pushOpdracht(self, naam, callback):
        self.wachtrij.append((naam, callback))

    def scrape(self):
        monitor = ScrapeMonitor(len(self.wachtrij), self.FIRST_ID)
        n = -1
        for opdracht in self.wachtrij:
            naam, callback = opdracht
            tweetaantal = 200
            data = []
            oudsteId = self.LAST_ID
            pagina = 0
            over = 299
            try:
                try:
                    while tweetaantal >= 195 and oudsteId >= self.FIRST_ID:
                        n = n + 1
                        if n >= len(self.twitters): n = 0
                        pagina = pagina + 1
                        newdata = self.twitters[n].get_user_timeline(screen_name=naam, count=200, since_id=self.FIRST_ID, max_id=oudsteId)
                        tweetaantal = len(newdata)
                        if tweetaantal > 0:
                            for tweet in newdata:
                                newid = int(tweet['id_str'])
                                if newid < oudsteId: oudsteId = newid
                            monitor.printSuccess(naam, pagina, tweetaantal, oudsteId, n, over)
                            data.extend(newdata)
                        else:
                            raise TwythonError('No data for this user')
                        over = int(self.twitters[n].get_lastfunction_header('x-rate-limit-remaining'))
                    callback(data, naam)
                except TwythonError, e:
                    monitor.printError(naam, e.message, n, over)
                    if e.error_code == 429:
                        over = 0
                    else:
                        over = int(self.twitters[n].get_lastfunction_header('x-rate-limit-remaining'))
                if over < 1:
                    reset = int(self.twitters[n].get_lastfunction_header('x-rate-limit-reset'))
                    wachttijd = reset - time.time() + 10
                    monitor.printWachten(reset, n, over)
                    time.sleep(wachttijd)
            except KeyboardInterrupt:
                raise
            except Exception, e:
                monitor.printError(naam, 'Unknown error: %s' % e.message, n, over)


# Dummy implementation

def dummmycallback(data, name):
    print data, name

def runTwitter():
    scraper = TimelineScraper(12348768)
    scraper.pushOpdracht('WageningenUR', callback=callback)