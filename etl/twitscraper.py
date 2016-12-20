__author__ = 'gijspeters'

import time
import twython
from twython import Twython

from misc.outputs import ScrapeMonitor

API_KEY = ['KvvFEg9TcDODoLr5YSSouVJMv',
           'GRHgkAzRuYCRkUci9v5SIzQnI',
           '6BOL8vfPC16TkMtC0VJxU88vB',
           'Bdy843FjZt48fwa9e4Alpm7ot',
           'zXlxCyKGaegi9VHEAKKqs7o0F',
           'ddYrZpf9nMosYZ18diaik7nCr',
           'Qna2IYU4lW1mqfQdujrsGSd28',]
API_SECRET = ['UQTiJdbS0HC3MBA4d4YyeZaiY881pbeRRGtKkoqbgmtzb29mKU',
              'y7DMJNsTuchS4NE16T6l9CICyDt73PjxFjTeDtkNuhTpJIQnmE',
              '52eHt8LvizgQgZa8GctmUDUpxqCDJ6BdpvzLb3HV5y3yQaUWcz',
              'DAVKqFPOK9cvZV0ypxEuBxHxSkSdF1sJ0qqczIZ1PRcdV8xuwP',
              'OWERg3ETOY1uK72NgfGuXNxA0w9noGL27pG9V5JUOTB8IkqBJ5',
              'PmFY16g771OBOBupmj3F9uVQPJEYZXa4tvR9NS4dmwgWqN9jqz',
              'GocS1wKHb2UxD1YPAKOdY3JHrkYwi7QQyr66KAgZm5JplMIoS6',]



class TimelineScraper:

    def __init__(self, firstId, lastId):
        self.twitters = []
        for i in range(7):
            token = Twython(API_KEY[i], API_SECRET[i], oauth_version=2).obtain_access_token()
            self.twitters.append(Twython(API_KEY[i], access_token=token))
        self.wachtrij = []
        self.FIRST_ID = firstId
        self.LAST_ID = lastId

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
                        if n > 6: n = 0
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
                            raise twython.TwythonError('Geen data voor deze gebruiker')
                        over = int(self.twitters[n].get_lastfunction_header('x-rate-limit-remaining'))
                    callback(data, naam)
                except twython.TwythonError, e:
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
                monitor.printError(naam, 'Onbekende error: %s' % e.message, n, over)
