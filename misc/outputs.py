__author__ = 'gijspeters'

import sys
import os
import time

class VoortgangRegel:

    MODUS_PROCENT = 0
    MODUS_NUM = 1

    def __init__(self, totaal, modus, tekst):
        self.modus = modus
        self.totaal = totaal
        self.tekst = tekst
        self.huidig = -1
        self.nieuweRegel()
        self.huidigeNaam = ""

    def nieuweRegel(self):
        if self.modus == VoortgangRegel.MODUS_PROCENT:
            sys.stdout.write('\n%s: 0 procent' % self.tekst)
        else:
            sys.stdout.write('\n%s: 0 / %d'% ( self.tekst, self.totaal ))
        sys.stdout.flush()

    def updateHuidig(self, huidig):
        self.huidig = huidig
        sys.stdout.write("\r")
        if self.modus == VoortgangRegel.MODUS_PROCENT:
            sys.stdout.write("%s: %d procent" % ( self.tekst, ((100*self.huidig)/self.totaal) ) )
        else:
            sys.stdout.write("%s: %d / %d" % ( self.tekst, self.huidig, self.totaal ))
        sys.stdout.flush()

    def printError(self, message):
        sys.stdout.write("\r%s\n" % message)
        sys.stdout.flush()

    def plusEen(self):
        self.updateHuidig(self.huidig + 1)

class ScrapeMonitor:

    def __init__(self, totaal, firstId):
        self.totaal = totaal
        self.firstId = firstId
        self.nieuweMonitor()
        self.huidig = -1
        self.huidigeNaam = ''

    def nieuweMonitor(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print "TwitScraper"
        print "Namen verwerkt: 0 / %d" % self.totaal
        print "Huidige naam:"
        print "Geen melding."

    def printSuccess(self, naam, pagina, aantal, oudsteId, n, r):
        self.updateRegel(naam, n, r)
        sys.stdout.write("Pagina %d met count:%d, oudste ID: %d" % (pagina, aantal, oudsteId))
        sys.stdout.flush()

    def printError(self, naam, message, n, r):
        self.updateRegel(naam, n, r)
        sys.stdout.write(message)
        sys.stdout.flush()

    def printWachten(self, seconden, n, r):
        self.updateRegel(self.huidigeNaam, n, r)
        sys.stdout.write("Rate limit op, vervolg scrapen om %s" % time.strftime("%H:%M:%S", time.localtime(seconden)))
        sys.stdout.flush()

    def updateRegel(self, naam, n, r):
        if naam != self.huidigeNaam:
            self.huidigeNaam = naam
            self.huidig = self.huidig + 1
        os.system('cls' if os.name == 'nt' else 'clear')
        sys.stdout.write("TwitScraper, Appnr: %d, %d requests over \n" % (n, r))
        sys.stdout.write("Namen verwerkt: %d / %d \n" % (self.huidig, self.totaal))
        sys.stdout.write("Huidige naam: %s \n" % naam)

class TrajectoryMonitor:

    def __init__(self, totaal):
        self.totaal = totaal
        self.huidig = 0
        self.naam = ""

    def nieuweMonitor(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print "Trajectories"
        print "Namen verwerkt: 0 / %d" % self.totaal
        return self

    def updateRegel(self, naam, begintijd, eindtijd):
        if naam != self.naam:
            self.naam = naam
            self.huidig += 1
        os.system('cls' if os.name == 'nt' else 'clear')
        sys.stdout.write("Trajectories\n")
        sys.stdout.write("Namen verwerkt: %d / %d\n" % (self.huidig, self.totaal))
        sys.stdout.write("%s: %s tot %s\n" % (naam, begintijd.ctime(), eindtijd.ctime()))
        sys.stdout.flush()

class HagerstrandMonitor:

    def __init__(self, totaal):
        self.totaal = totaal
        self.huidig = 0
        self.naam = ""
        self.nieuweMonitor()

    def nieuweMonitor(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        print "Hagerstrandprismas"
        print "Routes verwerkt: 0 / %d" % self.totaal
        return self

    def plusEen(self, queuesize):
        self.huidig += 1
        os.system('cls' if os.name == 'nt' else 'clear')
        sys.stdout.write("Hagerstrandprismas\n")
        sys.stdout.write("Routes verwerkt: %d / %d\n" % (self.huidig, self.totaal))
        sys.stdout.write("Items in queue: %d" % queuesize)
        sys.stdout.flush()