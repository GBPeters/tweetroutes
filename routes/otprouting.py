__author__ = 'gijspeters'

import dbconnect
import json
import urllib
import urllib2
from datetime import date, datetime
from polyline.codec import PolylineCodec

from db import dbtables
from misc import outputs

URL = 'http://localhost:8080/otp/routers/default/plan?'
MONDAY = datetime(2015, 10, 5)

def batchOTP():
    dbtables.OTPFAST.createTable()
    dbtables.OTPPREF.createTable()
    dbtables.OTPLEGS.createTable()
    con = dbconnect.Verbinding()
    sql = "UPDATE routes SET useful=null, isfast=null where useful is not null"
    con.exe(sql)
    sql = "SELECT routes.id, srct.lon, srct.lat, srct.tijddatum, tgtt.lon, tgtt.lat, realtime FROM routes " \
          "INNER JOIN rel_traj_tweets AS src ON routes.startrel=src.id " \
          "INNER JOIN rel_traj_tweets AS tgt ON routes.endrel=tgt.id " \
          "INNER JOIN scraped_tweets AS srct ON src.tweet_id=srct.id " \
          "INNER JOIN scraped_tweets AS tgtt ON tgt.tweet_id=tgtt.id "
    routes = con.selectAll(sql)
    monitor = outputs.VoortgangRegel(len(routes), outputs.VoortgangRegel.MODUS_NUM, 'Routes verwerkt')
    errors = 0
    for (id, startlon, startlat, startdate, endlon, endlat, realtime) in routes:
        monitor.plusEen()
        newDate = date(MONDAY.year, MONDAY.month, MONDAY.day + startdate.weekday())
        #Snelst mogelijke tijd opvragen
        params =  {'time' : '%s' % startdate.time(),
                   'fromPlace' : '%s,%s' % (startlat, startlon),
                   'toPlace' :   '%s,%s' % (endlat, endlon),
                   'mode' : 'WALK,TRANSIT',
                   'date' : newDate,
                   'walkSpeed' : 1.4,
                   'maxWalkDistance' : 1000}
        url = URL + urllib.urlencode(params)
        req = urllib2.Request(url)
        req.add_header('Accept', 'application/json')
        try :
            response = urllib2.urlopen(req)
        except urllib2.HTTPError as e :
            errors += 1
            print e
            continue
        try :
            content = response.read()
            objs = json.loads(content)
            plan = objs['plan']
        except Exception, e:
            errors += 1
            continue
        trip = plan['itineraries'][0]
        fduration = (trip['endTime'] - plan['date'])/1000
        fslow = realtime < 0.9*(fduration/3600)
        fastlegs = trip['legs']
        sql = "INSERT INTO otpfast (route_id, duration, tooslow) VALUES (%d, %d, %s);" \
              "UPDATE routes SET useful=%s WHERE id=%d" % (id, fduration, fslow, not(fslow), id)
        con.exe(sql)
        #Pref. route opvragen
        params =  {'time' : '%s' % startdate.time(),
                   'fromPlace' : '%s,%s' % (startlat, startlon),
                   'toPlace' :   '%s,%s' % (endlat, endlon),
                   'mode' : 'WALK,TRANSIT',
                   'date' : newDate,
                   'maxWalkDistance' : 2000}
        url = URL + urllib.urlencode(params)
        req = urllib2.Request(url)
        req.add_header('Accept', 'application/json')
        try :
            response = urllib2.urlopen(req)
        except urllib2.HTTPError as e :
            errors += 1
            print e
            continue
        try :
            content = response.read()
            objs = json.loads(content)
            plan = objs['plan']
        except Exception, e:
            errors += 1
            continue
        trip = plan['itineraries'][0]
        pduration = (trip['endTime'] - plan['date'])/1000
        pslow = realtime < 0.9*(pduration/3600)
        preflegs = trip['legs']
        sql = "INSERT INTO otppref (route_id, duration, tooslow) VALUES (%d, %d, %s);" % (id, pduration, pslow)
        con.exe(sql)
        if pduration < fduration:
            sql = "UPDATE otpfast SET duration=%d, tooslow=%s WHERE route_id=%d;" \
                  "UPDATE routes SET useful=%s WHERE id=%d" % (pduration, pslow, id, not(pslow), id)
            con.exe(sql)
            fslow = pslow
        isfast = not(fslow) and pslow
        sql = "UPDATE routes SET isfast=%s WHERE id=%d" % (isfast, id)
        con.exe(sql)
        if not(fslow):
            legs = preflegs
            if pslow:
                legs = fastlegs
            i = 0
            for leg in legs:
                geom = leg['legGeometry']['points']
                if len(geom) > 0:
                    pl = PolylineCodec().decode(geom)
                    linestring = 'LINESTRING('
                    for lat, lon in pl:
                        linestring = linestring + '%f %f,' % (lon, lat)
                    linestring = linestring[:-1] + ')'
                    sql = "INSERT INTO otplegs (route_id, legnum, points, mode, distance, duration) VALUES (%d, %d, ST_GeomFromText('%s', 4326), '%s', %f, %d)" \
                          % (id, i, linestring, leg['mode'], leg['distance'], leg['duration'] )
                    con.exe(sql)
    print 'Fouten: ', errors
    con.commit()
    con.sluit()

def createEdges():
    dbtables.OTPEDGES.createTable()
    con = dbconnect.Verbinding()
    sql = "SELECT id, mode FROM otplegs"
    ids = con.selectAll(sql)
    monitor = outputs.VoortgangRegel(len(ids), outputs.VoortgangRegel.MODUS_NUM, 'Legs verwerkt')
    for id, mode in ids:
        sql = "SELECT ST_DumpPoints(points) FROM otplegs WHERE id=%d" % (id)
        points = con.selectAll(sql)
        valuestr = ""
        for i in range(len(points)-1):
            valuestr = valuestr + "(%d, %d, ST_MakeLine('%s', '%s'), '%s'), " % (id, i, points[i][0].split(',')[1][:-1], points[i+1][0].split(',')[1][:-1], mode)
        sql = "INSERT INTO otpedges (leg_id, edgenum, geom, mode) VALUES %s" % valuestr[:-2]
        con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()

def createEdgeNetwork():
    print '\nNetwerk maken...'
    dbtables.OTPNETWORK.createTable()
    con = dbconnect.Verbinding()
    sql = "WITH n AS (" \
          "INSERT INTO otpnetwork (geom, mode, used) SELECT geom, mode, count(id) FROM otpedges " \
          "GROUP BY geom, mode " \
          "RETURNING geom g, mode m, id i) " \
          "UPDATE otpedges SET network_id=n.i FROM n " \
          "WHERE geom=n.g AND mode=n.m"
    con.exe(sql)
    con.commit()
    con.sluit()

def calcLegNums():
    con = dbconnect.Verbinding()
    sql = "select id, route_id from otplegs"
    legs = con.selectAll(sql)
    vorigeRoute = 0
    monitor = outputs.VoortgangRegel(len(legs), outputs.VoortgangRegel.MODUS_NUM, 'Legs verwerkt')
    for id, route in legs:
        if route == vorigeRoute:
            legnum += 1
        else:
            legnum = 0
        vorigeRoute = route
        sql = "UPDATE otplegs SET legnum=%d WHERE id=%d" % (legnum, id)
        con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()

