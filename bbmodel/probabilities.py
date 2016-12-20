import sys
from scipy.optimize.nonlin import NoConvergence
from scipy.stats import truncnorm

__author__ = 'gijspeters'

from db.dbconnect import Verbinding
from misc.outputs import VoortgangRegel
from db import dbtables
from math import sqrt, log, exp
from StringIO import StringIO
from scipy import optimize as opt
from multiprocessing import Process, Queue, JoinableQueue, Manager
from time import sleep

class LocationDeviatonSolver (object):

    def __init__(self, sb, sf):
        self.L = (sb + sf) * 1.0
        self.f = sb/self.L
        try:
            self.h = opt.broyden1(self.totalLengthSolver, [1], f_tol=.0001)[0]
        except:
            self.h = 0
            self.x = self.f
            self.y = 0
        else:
            try:
                self.x = opt.broyden1(self.partialLengthSolver, [0.25 + ((self.f > 0.5) * 0.5)], f_tol=0.001)[0]
                self.y = self.h * (1.0 - 4.0 * (self.x - 0.5) ** 2.0)
            except:
                self.x = .5
                self.y = self.h

    def totalLengthSolver(self, h):
        h = h[0]
        if h == 0:
            return 1 - self.L
        else:
            return 0.6250000000e-1 * (0.8e1 * h * sqrt(0.16e2 * h ** 2 + 0.1e1) - 0.1e1
            * log(sqrt(0.16e2 * h ** 2 + 0.1e1) - 0.4e1 * h) + log(0.4e1 * h
            + sqrt(0.16e2 * h ** 2 + 0.1e1))) / h - self.L

    def partialLengthSolver(self, a):
        a = a[0]
        h = self.h
        if h == 0:
            return a - self.f
        else:
            return 0.6250000000e-1 * (0.8e1 * h * sqrt(0.64e2 * a ** 2 * h ** 2 - 0.64e2
            * a * h ** 2 + 0.16e2 * h ** 2 + 0.1e1) * a - 0.4e1 * h * sqrt(0.64e2 * a
            ** 2 * h ** 2 - 0.64e2 * a * h ** 2 + 0.16e2 * h ** 2 + 0.1e1) + 0.4e1 * h *
            sqrt(0.16e2 * h ** 2 + 0.1e1) - 0.1e1 * log(sqrt(0.16e2 * h ** 2
            + 0.1e1) - 0.4e1 * h) + log(0.8e1 * a * h + sqrt(0.64e2 * a ** 2 * h
            ** 2 - 0.64e2 * a * h ** 2 + 0.16e2 * h ** 2 + 0.1e1) - 0.4e1 * h)) / h - self.f



class PrismIterator (object):

    def __init__(self, prism, starttime=-1, endtime=-1, stepsize=1):
        self.starttime = starttime
        self.endtime = endtime
        if starttime == -1:
            try:
                self.starttime = min([tb for tb, tf in prism.prism.values()])
            except:
                self.starttime = -1
        if endtime == -1:
            try:
                self.endtime = max([tf for tb, tf in prism.prism.values()])
            except:
                self.endtime = -1
        self.t = self.starttime
        self.stepsize = stepsize
        self.curedges = [(k, v[0], v[1]) for k, v in prism.prism.iteritems()]

    def __iter__(self):
        return self

    def next(self):
        if self.t > self.endtime or self.starttime == self.endtime == self.t == -1:
            raise StopIteration
        else:
            self.t += self.stepsize
            redges = [(k, tb, tf) for k, tb, tf in self.curedges if tb <= self.t <= tf]
            return (self.t - self.stepsize, redges)


class Prism (object):

    def __init__(self, edges):
        self.prism = {}
        for id, tb, tf in edges:
            self.prism[id] = (tb, tf)

    def getEdgeForId(self, id):
        return self.prism[id]

    def getEdgesAtTime(self, time):
        return [(k, v[0], v[1]) for k, v in self.prism.iteritems() if time >= v[0] and time <= v[1]]

    def getEdgesAtTimeRange(self, starttime, endtime):
        return [(k, v[0], v[1]) for k, v in self.prism.iteritems() if starttime >= v[0] and endtime <= v[1]]

    def getEdgesWithinTimeRange(self, starttime, endtime):
        return [(k, v[0], v[1]) for k, v in self.prism.iteritems() if starttime <= v[0] and endtime >= v[1]]

    def getEdgesIntersectingTimeRange(self, starttime, endtime):
        return [(k, v[0], v[1]) for k, v in self.prism.iteritems() if v[0] <= starttime <= v[1] or v[0] <= endtime <= v[1]]

    def setEdge(self, id, tb, tf):
        self.prism[id] = (tb, tf)

    def removeEdge(self, id):
        del self.prism[id]

    def popEdge(self, id):
        return self.prism.pop(id)

    def iteratePrism(self, starttime=-1, endtime=-1, stepsize=1):
        return PrismIterator(self, starttime, endtime, stepsize)




def startCalc():
    sql = 'SELECT route_id, ti, tj, dij, dji FROM hroutes'
    con = Verbinding()
    try:
        con.createTable(dbtables.PROBEDGES)
        routes = con.selectAll(sql)
    finally:
        con.sluit()
    calcProb(routes)

def continueCalc():
    sql = 'SELECT route_id, ti, tj, dij, dji FROM hroutes WHERE route_id not in (SELECT route_id FROM probedges GROUP BY route_id)'
    con = Verbinding()
    try:
        routes = con.selectAll(sql)
    finally:
        con.sluit()
    calcProb(routes)

class EdgeVar:

    def __init__(self, edgeid, Db, Df, tb = 0, tf = 0, x=0, y=0, P=.0, E=.0):
        self.edgeid = edgeid
        self.Db = Db
        self.Df = Df
        self.tb = tb
        self.tf = tf
        self.x = x
        self.y = y
        self.P = P
        self.E = E

class ParabolaWorker (Process):

    def __init__(self, tq, rq, Dij, ti, tj):
        Process.__init__(self)
        self.daemon = True
        self.tq = tq
        self.rq = rq
        self.Dij = Dij
        self.ti = ti
        self.tj = tj

    def run(self):
        Dij = self.Dij
        ti = self.ti
        tj = self.tj
        while True:
            if not self.tq.empty():
                edge = self.tq.get()
                if edge is None:
                    break
                else:
                    edgeid, Db, Df = edge
                    sb = Db/(Dij * 1.0)
                    sf = Df/(Dij * 1.0)
                    tb = ti + Db
                    tf = tj - Df
                    LDS = LocationDeviatonSolver(sb, sf)
                    x = LDS.x
                    y = LDS.y
                    self.rq.put((edgeid, tb, tf, x, y))
            else:
                sleep(.1)
        return


class IteratorWorker (Process):

    def __init__(self, tq, rq, ti, tj, vmax, vmean, edgevars):
        Process.__init__(self)
        self.daemon = True
        self.tq = tq
        self.rq = rq
        self.ti = ti
        self.tj = tj
        self.vmax = vmax
        self.vmean = vmean
        self.edgevars = edgevars

    def run(self):
        ti = self.ti
        tj = self.tj
        vmax = self.vmax
        vmean = self.vmean
        while True:
            if not self.tq.empty():
                edge = self.tq.get()
                if edge is None:
                    break
                else:
                    t, edgelist = edge
                    sigma = sqrt(((t-ti) * (tj-t))/(tj-ti))
                    mu = vmean * (t-ti)
                    timep = {}
                    Lx = max(1-vmax *(tj-t), -vmax * (t-ti))
                    Ux = min(vmax * (t-ti), vmax * (tj-t) + 1)
                    for k, d1, d2 in edgelist:
                        x = self.edgevars[k].x
                        if Lx <= x <= Ux:
                            y = self.edgevars[k].y
                            Uy = min(sqrt(Ux ** 2.0 - x ** 2.0), sqrt((1-Lx) ** 2.0 - (1-x) ** 2.0))
                            if y <= Uy:
                                Ly = -Uy
                                Px = truncnorm.pdf(x, (Lx - mu) / sigma, (Ux - mu) / sigma, loc=mu, scale=sigma)
                                Py = truncnorm.pdf(y, Ly / sigma, Uy / sigma, scale=sigma)
                                P = Px * Py
                                timep[k] = P
                    totalp = sum(timep.itervalues())
                    resultedges = {}
                    if totalp > 0:
                        normfactor = 1.0 / totalp
                        for k, v in timep.iteritems():
                            normp = timep[k] * normfactor
                            try:
                                P = log(1-normp) * 30
                            except:
                                P = log(.000001) * 30
                                pass
                            resultedges[k] = (P, normp)
                    self.rq.put(resultedges)
            else:
                sleep(.1)

class SumWorker (Process):

    def __init__(self, tq, rq, edgevars):
        Process.__init__(self)
        self.daemon = True
        self.tq = tq
        self.rq = rq
        self.edgevars = edgevars

    def run(self):
        while True:
            if not self.tq.empty():
                edges = self.tq.get()
                if edges is None:
                    break
                else:
                    for k, v in edges.iteritems():
                        P, E = v
                        self.edgevars[k].P += P
                        self.edgevars[k].E += E
            else:
                sleep(.1)
        self.rq.put(self.edgevars)


def calcProb(routes):
    i = 1
    for routeid, ti, tj, Dij, Dji in routes:
        print "Route %d/%d - ti: %d, tj: %d, Dij: %d, Dji: %d" % (i, len(routes), ti, tj, Dij, Dji)
        i += 1
        Dij = min (Dij, Dji)
        if Dij <= 0: Dij = 1
        # Edges ophalen
        con = Verbinding()
        try:
            sql = 'SELECT edge_id, db, df FROM prismedges WHERE route_id=%d' % routeid
            edges = con.selectAll(sql)
            # Route constants
            vmean = 1/((tj-ti)*1.0)
            vmax = 1/(Dij * 1.0)
            # XY uitrekenen
            edgevars = {}
            print 'Calculating XY...'
            manager = Manager()
            tq = manager.Queue()
            rq = manager.Queue()
            for edgeid, Db, Df in edges:
                if Db + Df == 0:
                    Df = 1
                tq.put((edgeid, Db, Df))
                edgevars[edgeid] = EdgeVar(edgeid, Db=Db, Df=Df)
            worker1 = ParabolaWorker(tq, rq, Dij, ti, tj)
            worker2 = ParabolaWorker(tq, rq, Dij, ti, tj)
            worker3 = ParabolaWorker(tq, rq, Dij, ti, tj)
            worker1.start()
            worker2.start()
            worker3.start()
            tq.put(None)
            tq.put(None)
            tq.put(None)
            worker1.join()
            worker2.join()
            worker3.join()
            while not rq.empty():
                edge = rq.get()
                k = edge[0]
                edgevars[k].tb, edgevars[k].tf, edgevars[k].x, edgevars[k].y = edge[1:]
            worker1.terminate()
            worker2.terminate()
            worker3.terminate()
            #Iterate over time
            print 'Iterating over time...'
            edges = [(k, v.tb, v.tf) for k, v in edgevars.iteritems()]
            prism = Prism(edges)
            tq = manager.Queue()
            rq = manager.Queue()
            sq = manager.Queue(1)
            for t, edgelist in prism.iteratePrism(stepsize=30):
                tq.put((t, edgelist))
            worker1 = IteratorWorker(tq, rq, ti, tj, vmax, vmean, edgevars)
            worker2 = IteratorWorker(tq, rq, ti, tj, vmax, vmean, edgevars)
            worker3 = IteratorWorker(tq, rq, ti, tj, vmax, vmean, edgevars)
            summator = SumWorker(rq, sq, edgevars)
            worker1.start()
            worker2.start()
            worker3.start()
            summator.start()
            tq.put(None)
            tq.put(None)
            tq.put(None)
            worker1.join()
            worker2.join()
            worker3.join()
            rq.put(None)
            summator.join(10)
            edgevars = sq.get()
            summator.terminate()
            worker1.terminate()
            worker2.terminate()
            worker3.terminate()
            #Copy edge to table
            routestr = ''
            for edge in edgevars.itervalues():
                if edge.P > 1.0 : edge.P = 1
                routestr += '%d\t%d\t%d\t%d\t%f\t%f\t%f\t%f\n' % (routeid, edge.edgeid, edge.Db, edge.Df, edge.x, edge.y, 1.0-exp(edge.P), edge.E)
            f = StringIO(routestr)
            con.copyfrom(f, 'probedges', columns=('route_id', 'edge_id', 'db', 'df', 'x', 'y', 'P', 'E'))
            con.commit()
        finally:
            con.sluit()