__author__ = 'gijspeters'

import calendar
from codecs import open
from com.vividsolutions.jts.geom import GeometryFactory, PrecisionModel
from com.vividsolutions.jts.io import WKBWriter
from com.vividsolutions.jts.util import AssertionFailedException
from datetime import datetime, date
from jdbconnect import Verbinding, CopyVerbinding
from org.opentripplanner.routing.core import State
from org.opentripplanner.scripting.api import *
from random import randint

from db import dbtables
from misc.outputs import HagerstrandMonitor, VoortgangRegel

MONDAY = datetime(2015, 10, 5)

class hstate:

    def createhstate(self):
        if self.srcstate is not None and self.tgtstate is not None:
            if self.srcstate.getVertex().getLabel() != self.tgtstate.getVertex().getLabel():
                raise AttributeError("Source vertex does not match target vertex")

    def __init__(self, srcstate, tgtstate):
        self.srcstate = srcstate
        self.tgtstate = tgtstate
        self.createhstate()

    def __init__(self, statetuple):
        self.srcstate, self.tgtstate = statetuple
        self.createhstate()

    def setBackHStates(self, hstatedict):
        backstate = self.srcstate.getBackState()
        forwardstate = self.tgtstate.getBackState()
        try:
            self.backhstate = hstatedict[backstate.getVertex().getIndex()]
        except (KeyError, AttributeError):
            if backstate == None:
                self.backhstate = None
        try:
            self.forwardhstate = hstatedict[forwardstate.getVertex().getIndex()]
        except (KeyError, AttributeError):
            self.forwardhstate = None

    def getTimeLeft(self):
        return self.getMaxDepartTime()-self.getMinArriveTime()

    def getMinArriveTime(self):
        return self.srcstate.getTimeSeconds()

    def getMaxDepartTime(self):
        return self.tgtstate.getTimeSeconds()

    def getVertexLat(self):
        return self.srcstate.getVertex().getLat()

    def getVertexLon(self):
        return self.srcstate.getVertex().getLon()

    def getVertexGeom(self):
        return WKBWriter().bytesToHex(WKBWriter().write(GeometryFactory(PrecisionModel(), 4326).createPoint(self.srcstate.getVertex().getCoordinate())))

    def getBackHState(self):
        return self.backhstate

    def getForwardHState(self):
        return self.forwardhstate

    def getBackEdgeGeom(self):
        try:
            return WKBWriter().bytesToHex(WKBWriter().write(self.srcstate.getBackEdge().getGeometry()))
        except (AttributeError, AssertionFailedException):
            return ''

    def getForwardEdgeGeom(self):
        try:
            return WKBWriter().bytesToHex(WKBWriter().write(self.tgtstate.getBackEdge().getGeometry()))
        except (AttributeError, AssertionFailedException):
            return ''

    def getBackMode(self):
        try:
            return self.srcstate.getBackMode().name()
        except AttributeError:
            return ''

    def getForwardMode(self):
        try:
            return self.tgtstate.getBackMode().name()
        except AttributeError:
            return ''

    def getBackDuration(self):
        return self.srcstate.getTimeDeltaSeconds()

    def getForwardDuration(self):
        return self.tgtstate.getTimeDeltaSeconds()

    def getBackDistance(self):
        try:
            return self.srcstate.getBackEdge().getDistance()
        except AttributeError:
            return 0

    def getForwardDistance(self):
        try:
            return self.tgtstate.getBackEdge().getDistance()
        except AttributeError:
            return 0

    def getLabel(self):
        return self.srcstate.getVertex().getLabel()

    def getIndex(self):
        return 0

    def getVertexIndex(self):
        return self.srcstate.getVertex().getIndex()

    def getBackIndex(self):
        if not hasattr(self, 'backhstate'):
            raise AttributeError('Backstate not set. Please set back- and forwardstates using setBackHState(hstatelist)')
        if self.backhstate == None:
            return -1
        else:
            return self.backhstate.getIndex()

    def getForwardIndex(self):
        if not hasattr(self, 'backhstate'):
            raise AttributeError('Forwardstate not set. Please set back- and forwardstates using setBackHState(hstatelist)')
        if self.forwardhstate == None:
            return -1
        else:
            return self.forwardhstate.getIndex()

    def getDataTuple(self):
        return (self.getVertexIndex(),
                self.getBackIndex(),
                self.getForwardIndex(),
                self.getLabel(),
                self.getVertexGeom(),
                self.getBackEdgeGeom(),
                self.getForwardEdgeGeom(),
                self.getBackDistance(),
                self.getForwardDistance(),
                self.getMinArriveTime(),
                self.getMaxDepartTime(),
                self.getTimeLeft(),
                self.getBackDuration(),
                self.getForwardDuration(),
                self.getBackMode(),
                self.getForwardMode())

class HEdge:

    def __init__(self, state):
        self.edge = state.getBackEdge()
        self.duration = state.getTimeDeltaSeconds()
        try:
            self.mode = state.getBackMode().name()
        except AttributeError:
            self.mode = None
        self.minarrivetime = self.maxdeparttime = 0

    def getIndex(self):
        return self.edge.getId()

    def getFromId(self):
        return self.edge.getFromVertex().getIndex()

    def getToId(self):
        return self.edge.getToVertex().getIndex()

    def getGeom(self):
        try:
            return WKBWriter().bytesToHex(WKBWriter().write(self.edge.getGeometry()))
        except (AttributeError, AssertionFailedException):
            return ''

    def getDataTuple(self):
        return (self.getIndex(),
                self.getFromId(),
                self.getToId(),
                self.getGeom(),
                self.minarrivetime,
                self.maxdeparttime,
                self.duration,
                self.mode)

class HVertex:

    def __init__(self, srcstate, tgtstate):
        self.vertex = srcstate.getVertex()
        self.minarrivetime = srcstate.getTimeSeconds()
        self.maxdeparttime = tgtstate.getTimeSeconds()
        self.backduration = self.minarrivetime - srcstate.getOptions().getDateTime().getTime()/1000
        self.forwardduration = tgtstate.getOptions().getDateTime().getTime()/1000 - self.maxdeparttime
        self.edges = [HEdge(State(self.vertex, edge, self.minarrivetime, srcstate.getOptions())) for edge in self.vertex.getOutgoing().toArray()]

    def getIndex(self):
        return self.vertex.getIndex()

    def getLabel(self):
        return self.vertex.getLabel()

    def getGeom(self):
        return WKBWriter().bytesToHex(WKBWriter().write(GeometryFactory(PrecisionModel(), 4326).createPoint(self.vertex.getCoordinate())))

    def getTimeLeft(self):
        return self.maxdeparttime - self.minarrivetime

    def getDataTuple(self):
        return (self.getIndex(),
                self.getLabel(),
                self.getGeom(),
                self.minarrivetime,
                self.maxdeparttime,
                self.backduration,
                self.forwardduration)


def createOTPRoutes():
    try:
        con = Verbinding()
        con.createTable(dbtables.OTPROUTES)
        sql = "SELECT DISTINCT traj_id FROM rel_traj_tweets"
        trajs = con.selectAll(sql)
        monitor = VoortgangRegel(len(trajs), VoortgangRegel.MODUS_NUM, 'Trajectories verwerkt')
        errors = 0
        for traj, in trajs:
            sql = "SELECT rel_traj_tweets.id, rel_traj_tweets.tweet_id, scraped_tweets.tijddatum FROM rel_traj_tweets " \
                  "INNER JOIN scraped_tweets ON rel_traj_tweets.tweet_id=scraped_tweets.id " \
                  "WHERE traj_id= %d ORDER BY scraped_tweets.tijddatum " % (traj)
            tweets = con.selectAll(sql)
            i = 0
            for i in range(len(tweets[:-1])):
                srcid, srctweet, srctime = tweets[i]
                tgtid, tgttweet, tgttime = tweets[i+1]
                try:
                    sql= "INSERT INTO otproutes (traj_id, route_num, startrel, endrel, starttweet, endtweet, realtime) " \
                         "VALUES (%d, %d, %d, %d, %d, %d, %d)" % (traj, i, srcid, tgtid, srctweet, tgttweet, (tgttime-srctime).total_seconds())
                    con.exe(sql)
                except TypeError:
                    errors +=1
            monitor.plusEen()
        print 'Errors: %d' % errors
        con.commit()
    finally:
        con.sluit()

def createOTPGraphTables():
    otp = OtpsEntryPoint.fromArgs(["--graphs", "/Users/gijspeters/otp/amsterdam", "--autoScan"])
    router = otp.getRouter()
    req = otp.createRequest()
    req.setDateTime(MONDAY.year, MONDAY.month, MONDAY.day, 12, 0, 0)
    req.setMaxTimeSec(14400)
    req.setOrigin(52.37, 4.889)
    sspt = router.plan(req)
    spt = sspt.spt
    vertices = spt.getVertices().toArray()
    con = Verbinding()
    con.createTable(dbtables.HEDGES)
    con.createTable(dbtables.HVERTICES)

    try:
        monitor = VoortgangRegel(len(vertices), VoortgangRegel.MODUS_NUM, 'Vertices geschreven')
        for vertex in vertices:
            dbvertex = (vertex.getIndex(), vertex.getLabel(), WKBWriter().bytesToHex(WKBWriter().write(GeometryFactory(PrecisionModel(), 4326).createPoint(vertex.getCoordinate()))))
            sql = "INSERT INTO hvertices (vertex_id, label, geom) VALUES (%d, '%s', '%s')" % dbvertex
            con.exe(sql)
            monitor.plusEen()
        con.commit()
        sql = "SELECT vertex_id, id FROM hvertices"
        hvertices = dict(con.selectAll(sql))
        monitor = VoortgangRegel(len(vertices), VoortgangRegel.MODUS_NUM, 'Edges geschreven van vertices')
        for vertex in vertices:
            edges = vertex.getOutgoing().toArray()
            for edge in edges:
                naam = edge.getName()
                if naam == None:
                    naam = ''
                else:
                    naam = naam.replace("'", "")
                try:
                    geom = WKBWriter().bytesToHex(WKBWriter().write(edge.getGeometry()))
                    hedge = (edge.getId(), naam, hvertices[vertex.getIndex()], hvertices[edge.getToVertex().getIndex()],
                         geom, edge.getDistance())
                    sql = "INSERT INTO hedges (edge_id, label, from_id, to_id, geom, distance) VALUES (%d, '%s', %d, %d, '%s', %f)" % hedge
                except:
                    hedge = (edge.getId(), naam, hvertices[vertex.getIndex()], hvertices.get(edge.getToVertex().getIndex(),-1), edge.getDistance())
                    sql = "INSERT INTO hedges (edge_id, label, from_id, to_id, distance) VALUES (%d, '%s', %d, %d, %f)" % hedge
                con.exe(sql)
            monitor.plusEen()
        con.commit()
    finally:
        con.sluit()


def continuePrisms():
    con = Verbinding()
    sql = "SELECT otproutes.id, ST_X(src.the_geom), ST_Y(src.the_geom), src.tijddatum, " \
          "ST_X(tgt.the_geom), ST_Y(tgt.the_geom), tgt.tijddatum, realtime FROM otproutes " \
          "INNER JOIN scraped_tweets AS src ON otproutes.starttweet=src.id " \
          "INNER JOIN scraped_tweets AS tgt ON otproutes.endtweet=tgt.id " \
          "WHERE src.tijddatum >= '2014-04-01'::date AND src.tijddatum < '2014-10-01'::date " \
          "AND otproutes.realtime <= 3600 " \
          "AND otproutes.id NOT IN (SELECT route_id FROM hroutes)"
    routes = con.selectAll(sql)
    con.sluit()
    createPrisms(routes)

def startPrisms():
    # routedata uit database halen
    con = Verbinding()
    con.createTable(dbtables.HROUTES)
    con.createTable(dbtables.PRISMEDGES)
    sql = "SELECT otproutes.id, ST_X(src.the_geom), ST_Y(src.the_geom), src.tijddatum, " \
          "ST_X(tgt.the_geom), ST_Y(tgt.the_geom), tgt.tijddatum, realtime FROM otproutes " \
          "INNER JOIN scraped_tweets AS src ON otproutes.starttweet=src.id " \
          "INNER JOIN scraped_tweets AS tgt ON otproutes.endtweet=tgt.id " \
          "WHERE src.tijddatum >= '2014-04-01'::date AND src.tijddatum < '2014-10-01'::date " \
          "AND otproutes.realtime <= 3600"
    routes = con.selectAll(sql)
    con.sluit()
    createPrisms(routes)

def createVertexAndEdgeTables():
    con = Verbinding()
    try:
        #con.createTable(dbtables.HVERTICES)
        con.createTable(dbtables.HEDGES)
        otp = OtpsEntryPoint.fromArgs(["--graphs", "/Users/gijspeters/otp/amsterdam", "--autoScan"])
        router = otp.getRouter().getRouter()
        graph = router.graph
        edges = graph.getStreetEdges().toArray()

        #Uncomment below to include vertices
        monitor = VoortgangRegel(len(edges), VoortgangRegel.MODUS_NUM, 'Edges verwerkt')
        errors = 0
        for edge in edges:
            if edge.getDistance() > 0:
                try:
                    geom = WKBWriter().bytesToHex(WKBWriter().write(edge.getGeometry()))
                except:
                    monitor.printError('Geen geometry')
                    errors += 1
                else:
                    sql = "INSERT INTO hedges (edge_id, label, from_id, to_id, geom, distance, midpoint) VALUES (%d, '%s', %d, %d, '%s', %f, ST_Line_Interpolate_Point('%s', 0.5))" \
                          % (edge.getId(), edge.getName(), edge.getFromVertex().getIndex(), edge.getToVertex().getIndex(), geom, edge.getDistance(), geom)
                    try:
                        con.exe(sql)
                    except:
                        errors += 1
            monitor.plusEen()
            con.commit()
        print 'Errors: %d' % errors
    finally:
        con.sluit()

def createPrisms(routes):
    # Graphdict opzetten
    con = Verbinding()
    try:
        sql = 'SELECT edge_id, ST_X(midpoint), ST_Y(midpoint) FROM huedges'
        graphedges = con.selectAll(sql)
    finally:
        con.sluit()
    graphmidpoints = {}
    for index, lon, lat in graphedges:
        graphmidpoints[index] = (lat, lon)
    # OTP entry point en router starten
    otp = OtpsEntryPoint.fromArgs(["--graphs", "/Users/gijspeters/otp/amsterdam", "--autoScan"])
    router = otp.getRouter()
    #Loop
    monitor = VoortgangRegel(len(routes), VoortgangRegel.MODUS_NUM, 'Routes verwerkt')
    for id, srclon, srclat, srctijd, tgtlon, tgtlat, tgttijd, realtime in routes:
        # Request voor origin
        newSrcDate = datetime(MONDAY.year, MONDAY.month, MONDAY.day + srctijd.weekday(), srctijd.hour, srctijd.minute, srctijd.second)
        req = otp.createRequest()
        req.setDateTime(long((newSrcDate-datetime(1970,1,1)).total_seconds()))
        req.setMaxTimeSec(long(realtime))
        req.setOrigin(srclat, srclon)
        try:
            srcsspt = router.plan(req)
        except:
            monitor.plusEen()
            continue
        if srcsspt != None:
            srcspt = srcsspt.spt
            # Request voor destination
            newTgtDate = datetime(MONDAY.year, MONDAY.month, MONDAY.day + tgttijd.weekday(), tgttijd.hour, tgttijd.minute, tgttijd.second)
            req = otp.createRequest()
            req.setArriveBy(True)
            req.setDateTime(long((newTgtDate-datetime(1970,1,1)).total_seconds()))
            req.setMaxTimeSec(long(realtime))
            req.setDestination(tgtlat, tgtlon)
            try:
                tgtsspt = router.plan(req)
            except:
                monitor.plusEen()
                continue
            if tgtsspt != None:
                tgtspt = tgtsspt.spt
                # Evaluate shortest route
                ti = calendar.timegm(srctijd.utctimetuple())
                tj = calendar.timegm(tgttijd.utctimetuple())
                try:
                    Dji = tgtsspt.eval(srclat, srclon).getTime()
                    Dij = srcsspt.eval(tgtlat, tgtlon).getTime()
                except:
                    monitor.plusEen()
                    continue
                # Find prism vertices
                srcvertices = srcspt.getVertices()
                tgtvertices = tgtspt.getVertices()
                srcids = set([v.getIndex() for v in srcvertices])
                tgtids = set([v.getIndex() for v in tgtvertices])
                iids = srcids.intersection(tgtids)
                prismvertices = []
                for v in [v for v in srcvertices if v.getIndex() in iids]:
                    try:
                        if srcspt.getState(v).getTimeSeconds() <= tgtspt.getState(v).getTimeSeconds():
                            prismvertices.append(v)
                    except:
                        pass
                # Get Prism Edges
                prismedges = []
                idset = set(graphmidpoints.keys())
                for v in prismvertices:
                    prismedges.extend([edge.getId() for edge in v.getOutgoingStreetEdges() if edge.getId() in idset])
                # Evaluate edge times
                edgetimes = {}
                for e in prismedges:
                    lat, lon = graphmidpoints[e]
                    try:
                        tb = srcsspt.eval(lat, lon).getTime()
                        tf = tgtsspt.eval(lat, lon).getTime()
                        if tb+tf <= realtime:
                            edgetimes[e] = (tb, tf)
                    except:
                        pass
                con = Verbinding()
                try:
                    sql = 'INSERT INTO hroutes (route_id, ti, tj, Dij, Dji) VALUES (%d, %d, %d, %d, %d)' % (id, ti, tj, Dij, Dji)
                    con.exe(sql)
                    con.commit()
                finally:
                    con.sluit()
                con = CopyVerbinding()
                try:
                    cm = con.getCopyStream("COPY prismedges (edge_id, route_id, db, df) FROM STDIN DELIMITER ';'")
                    for k, v in edgetimes.iteritems():
                        copyline = '%d;%d;%d;%d\n' % (k, id, v[0], v[1])
                        cm.writeToCopy(copyline, 0, len(copyline))
                    cm.endCopy()
                    con.commit()
                finally:
                    con.sluit()
                monitor.plusEen()
