__author__ = 'gijspeters'

import dbconnect
import psycopg2

from db import dbtables
from misc import outputs


def createStopTable():
    dbtables.STOPS.createTable()
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT id, geom FROM haltes"
    kursor.execute(sql)
    haltes = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(haltes), outputs.VoortgangRegel.MODUS_NUM, 'Haltes verwerkt')
    for halte in haltes:
        sql = "SELECT string_agg FROM src_tracksatstop WHERE pointref=%d" % halte[0]
        kursor.execute(sql)
        lijnen = kursor.fetchone()[0].split(';')
        for lijn in lijnen:
            sql = "INSERT INTO stops (pointref, geom, lijnnaam) VALUES (%d, '%s', '%s')" % (halte[0], halte[1], lijn)
            kursor.execute(sql)
        monitor.plusEen()
    sql = "DELETE FROM stops USING lijnen WHERE stops.lijnnaam=lijnen.lijnnaam AND lijnen.type='TRAIN'"
    kursor.execute(sql)
    sql = "SELECT station, lon, lat FROM treinverbindingen INNER JOIN stations_ns ON station=naam"
    kursor.execute(sql)
    stations = kursor.fetchall()
    for station, lon, lat in stations:
        sql = "INSERT INTO stops (pointref, geom, lijnnaam) SELECT max(pointref)+1, ST_SetSRID(ST_MakePoint(%f, %f), 4326), 'trein' " \
              "FROM stops RETURNING pointref" % (lon, lat)
        kursor.execute(sql)
        pointref = kursor.fetchone()[0]
        sql = "UPDATE treinverbindingen SET pointref=%d " \
              "WHERE station='%s'" % (pointref, station)
        kursor.execute(sql)
    kursor.connection.commit()
    kursor.close()



def createStations():
    dbtables.STATIONS.createTable()
    con = dbconnect.Verbinding()
    sql = "SELECT pointref, max(geom) FROM stops GROUP BY pointref"
    haltes = con.selectAll(sql)
    monitor = outputs.VoortgangRegel(len(haltes), outputs.VoortgangRegel.MODUS_NUM, 'Haltes verwerkt')
    for pointref, geom in haltes:
        sql = "SELECT ways.id, ways.osm_name, ways.source, ways.target, ways.x1, ways.y1, ways.x2, ways.y2, ways.geom_way, " \
              "ST_line_locate_point(ways.geom_way, '%s'), " \
              "ST_line_interpolate_point(ways.geom_way, ST_line_locate_point(ways.geom_way, '%s')), " \
              "ST_Line_Substring(ways.geom_way, 0, ST_line_locate_point(ways.geom_way, '%s'))," \
              "ST_Line_Substring(ways.geom_way, ST_line_locate_point(ways.geom_way, '%s'), 1) FROM " \
              "(SELECT subset.id, subset.osm_name, subset.source, subset.target, subset.km, subset.x1, subset.y1, subset.x2, subset.y2, subset.geom_way " \
              "FROM (SELECT id, osm_name, source, target, km, x1, y1, x2, y2, geom_way FROM ptrouting WHERE ST_DWithin('%s', geom_way, 0.005)) AS subset " \
              "ORDER BY ST_Distance(subset.geom_way, '%s') ASC LIMIT 1) AS ways" % (geom, geom, geom, geom, geom, geom)
        id, name, source, target, x1, y1, x2, y2, waygeom, frac, stationgeom, waysub1, waysub2 = con.selectOne(sql)
        sql = "INSERT INTO stations (pointref, geom) VALUES (%d, '%s'); " \
              "INSERT INTO ptvertices (node_id, lon, lat, geom, node_type, station_id) SELECT max(node_id)+1, ST_X('%s'), " \
              "ST_Y('%s'), '%s', 'station', %d FROM ptvertices RETURNING node_id; " % (pointref, stationgeom, stationgeom, stationgeom, stationgeom, pointref)
        node = con.selectOne(sql)[0]
        if frac == 0 or frac == 1:
            vertex = source
            if frac == 1: vertex = target
            sql = "UPDATE ptvertices SET node_type='station', station_id=%d WHERE node_id=%d" % (node, vertex)
            con.exe(sql)
        else:
            sql = "INSERT INTO ptrouting ( source, target, km, kmh, x1, y1, x2, y2, geom_way, edge_type) " \
                  "VALUES ( %d, %d, ST_Length_Spheroid('%s', 'SPHEROID[\"WGS 84\",6378137,298.257223563]')/1000, 5, %f, %f, ST_X('%s'), ST_Y('%s'), '%s', 'street'); " \
                  "INSERT INTO ptrouting ( source, target, km, kmh, x1, y1, x2, y2, geom_way, edge_type) " \
                  "VALUES ( %d, %d, ST_Length_Spheroid('%s', 'SPHEROID[\"WGS 84\",6378137,298.257223563]')/1000, 5, ST_X('%s'), ST_Y('%s'), %f, %f, '%s', 'street'); " \
                  "DELETE FROM ptrouting WHERE id=%d" \
                  % ( source, node, waysub1, x1, y1, stationgeom, stationgeom, waysub1,
                      node, target, waysub2, stationgeom, stationgeom, x2, y2, waysub2,
                     id)
            con.exe(sql)
        monitor.plusEen()
    sql = "UPDATE ptrouting SET cost=(km/5), reverse_cost=(km/5) WHERE cost IS NULL"
    con.exe(sql)
    con.commit()
    con.sluit()


def createPlatforms():
    con = dbconnect.Verbinding()
    sql = "DELETE FROM ptrouting WHERE edge_type='platform'; SELECT stops.id, pointref, stops.geom FROM stops INNER JOIN ptvertices ON pointref=station_id"
    stops = con.selectAll(sql)
    monitor = outputs.VoortgangRegel(len(stops), outputs.VoortgangRegel.MODUS_NUM, 'Stops verwerkt')
    fouten = 0
    for id, pointref, geom in stops:
        sql = "SELECT node_id, geom, lon, lat FROM ptvertices WHERE node_type='station' AND station_id=%d" % pointref
        res = con.selectAll(sql)
        if len(res) > 0:
            stationId, stationGeom, lon, lat = res[0]
            sql = "INSERT INTO ptvertices (node_id, lon, lat, geom, node_type, station_id, stop_id) " \
                  "SELECT max(node_id)+1, ST_X('%s'), ST_Y('%s'), '%s', 'stop', %d, %d FROM ptvertices " \
                  "RETURNING node_id" % (geom, geom, geom, pointref, id)
            node = con.selectOne(sql)[0]
            sql = "INSERT INTO ptrouting (source, target, km, kmh, x1, y1, x2, y2, geom_way, edge_type) " \
                  "VALUES (%d, %d, ST_Length_Spheroid(ST_MakeLine('%s', '%s'), 'SPHEROID[\"WGS 84\",6378137,298.257223563]')/1000" \
                  ", 5, %f, %f, ST_X('%s'), ST_Y('%s'), ST_MakeLine('%s', '%s'), 'platform')" \
                   % (stationId, node, stationGeom, geom, lon, lat, geom, geom, stationGeom, geom)
            con.exe(sql)
        else:
            fouten += 1
        con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()


def copyRoutingTable():
    con = dbconnect.Verbinding()
    sql = "DROP TABLE IF EXISTS ptrouting; SELECT * INTO ptrouting FROM routing; ALTER TABLE ptrouting OWNER TO postgres; " \
          "ALTER TABLE ptrouting ADD CONSTRAINT ptrouting_pkey PRIMARY KEY (id); " \
          "ALTER TABLE ptrouting ADD COLUMN edge_type character varying(20);" \
          "ALTER TABLE ptrouting DROP COLUMN osm_id, DROP COLUMN osm_meta, DROP COLUMN osm_source_id, " \
          "DROP COLUMN osm_target_id, DROP COLUMN clazz, DROP COLUMN flags; " \
          "ALTER TABLE ptrouting DROP COLUMN id, ADD COLUMN id serial NOT NULL;" \
          "UPDATE ptrouting SET edge_type='street', kmh=5 "
    con.exe(sql)
    con.commit()
    dbtables.PTVERTICES.createTable()
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT source, max(x1), max(y1) FROM ptrouting GROUP BY source"
    kursor.execute(sql)
    sources = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(sources), outputs.VoortgangRegel.MODUS_NUM, 'Sources verwerkt')
    for source in sources:
        sql = "INSERT INTO ptvertices (node_id, lon, lat, geom) VALUES (%d, %f, %f, ST_SetSRID(ST_MakePoint(%f, %f), 4326))" % (source[0], source[1], source[2], source[1], source[2])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    sql = "SELECT target, max(x2), max(y2) FROM ptrouting LEFT JOIN ptvertices ON ptrouting.target=ptvertices.node_id WHERE ptvertices.node_id IS NULL GROUP BY target"
    kursor.execute(sql)
    targets = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(targets), outputs.VoortgangRegel.MODUS_NUM, 'Targets verwerkt')
    for target in targets:
        sql = "INSERT INTO ptvertices (node_id, lon, lat, geom) VALUES (%d, %f, %f, ST_SetSRID(ST_MakePoint(%f, %f), 4326))" % (target[0], target[1], target[2], target[1], target[2])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    kursor.close()


def createLijnTabel():
    dbtables.LIJNEN.createTable()
    con = dbconnect.Verbinding()
    sql = "INSERT INTO lijnen (lijnnaam, type) SELECT lijnnaam, max(transportm) " \
          "FROM stops LEFT JOIN ptnet ON lijnnaam=publiccode GROUP BY lijnnaam ;" \
          "UPDATE lijnen SET type='BUS' WHERE type IS NULL;" \
          "INSERT INTO lijnen (lijnnaam, type) VALUES ('trein', 'TRAIN')"
    con.exe(sql)
    con.commit()
    con.sluit()


def createTracks():
    con = dbconnect.Verbinding()
    sql = "DELETE FROM ptrouting WHERE edge_type<>'street' AND edge_type<>'platform'"
    con.exe(sql)
    sql = "SELECT srctabel.node_id, tgttabel.node_id, distance, srctabel.geom, tgttabel.geom, lijnen.lijnnaam FROM src_tracks " \
          "INNER JOIN ptvertices AS srctabel ON pointref=srctabel.station_id " \
          "INNER JOIN ptvertices AS tgttabel ON pointref_1=tgttabel.station_id " \
          "INNER JOIN stops AS srcstop ON srctabel.stop_id=srcstop.id " \
          "INNER JOIN stops AS tgtstop ON tgttabel.stop_id=tgtstop.id " \
          "INNER JOIN lijnen ON srcstop.lijnnaam=lijnen.lijnnaam " \
          "WHERE srcstop.lijnnaam=tgtstop.lijnnaam "
    tracks = con.selectAll(sql)
    monitor = outputs.VoortgangRegel(len(tracks), outputs.VoortgangRegel.MODUS_NUM, 'Tracks verwerkt')
    for source, target, distance, srcgeom, tgtgeom, lijn in tracks:
        sql = "INSERT INTO ptrouting (source, target, km, x1, y1, x2, y2, geom_way, edge_type) " \
              "SELECT %d, %d, %f/1000, ST_X('%s'), ST_Y('%s'), ST_X('%s'), ST_Y('%s')," \
              "ST_MakeLine('%s', '%s'), type FROM lijnen WHERE lijnnaam='%s'" \
              % (source, target, distance, srcgeom, srcgeom, tgtgeom, tgtgeom, srcgeom, tgtgeom, lijn)
        con.exe(sql)
        monitor.plusEen()
    print ''
    print 'Treinverbindingen leggen...'
    sql = "SELECT station, doelstations, ptvertices.node_id, stops.geom FROM treinverbindingen " \
          "INNER JOIN stops ON treinverbindingen.pointref=stops.pointref " \
          "INNER JOIN ptvertices ON stops.id=ptvertices.stop_id"
    stations = con.selectAll(sql)
    for station, doelen, source, geom in stations:
        for doel in doelen.split(';'):
            sql = "INSERT INTO ptrouting (source, target, km, x1, y1, x2, y2, geom_way, edge_type) " \
                  "SELECT %d, ptvertices.node_id, ST_Length_Spheroid(ST_MakeLine('%s', stops.geom), 'SPHEROID[\"WGS 84\",6378137,298.257223563]')/1000, " \
                  "ST_X('%s'), ST_Y('%s'), ST_X(stops.geom), ST_Y(stops.geom), ST_MakeLine('%s', stops.geom), 'TRAIN' " \
                  "FROM treinverbindingen INNER JOIN stops ON treinverbindingen.pointref=stops.pointref " \
                  "INNER JOIN ptvertices ON stops.id=ptvertices.stop_id " \
                  "WHERE station='%s'" % (source, geom, geom, geom, geom, doel)
            con.exe(sql)
    con.commit()
    con.sluit()

def setCost():
    con = dbconnect.Verbinding()
    #print 'Straatkosten berekenen...'
    #sql = "UPDATE ptrouting SET cost=(km/5), reverse_cost=(km/5) WHERE edge_type='street'"
    #con.exe(sql)
    print 'OV-kosten berekenen...'
    sql = "UPDATE ptrouting SET cost=(km/10), reverse_cost=1000000 WHERE edge_type='BOAT'; " \
          "UPDATE ptrouting SET cost=(km/30), reverse_cost=1000000 WHERE edge_type='BUS'; " \
    	  "UPDATE ptrouting SET cost=(km/30), reverse_cost=1000000 WHERE edge_type='TRAM'; " \
    	  "UPDATE ptrouting SET cost=(km/60), reverse_cost=1000000 WHERE edge_type='METRO'; " \
          "UPDATE ptrouting SET cost=(km/80), reverse_cost=1000000 WHERE edge_type='TRAIN' "
    con.exe(sql)
    print 'Platformkosten berekenen...'
    sql = "UPDATE ptrouting SET cost=(0.0833333), reverse_cost=(km/5) WHERE edge_type='platform' "
    con.exe(sql)
    con.commit()
    con.sluit()


def nearestNodes():
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT rel_traj_tweets.id, the_geom FROM scraped_tweets INNER JOIN rel_traj_tweets ON scraped_tweets.id=rel_traj_tweets.tweet_id ORDER BY traj_id ASC"
    kursor.execute(sql)
    tweets = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(tweets), outputs.VoortgangRegel.MODUS_NUM, 'Tweets verwerkt')
    for tweet in tweets:
        sql = "UPDATE rel_traj_tweets SET node=nodes.source FROM " \
              "(SELECT subset.source FROM (SELECT source, geom_way FROM ptrouting WHERE ST_DWithin('%s', geom_way, 0.01)) AS subset " \
              "ORDER BY ST_Distance(subset.geom_way, '%s') ASC LIMIT 1) AS nodes " \
              "WHERE rel_traj_tweets.id=%d" % (tweet[1], tweet[1], tweet[0])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    kursor.close()



def createRouteTable():
    dbtables.ROUTES.createTable()
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT traj_id FROM rel_traj_tweets GROUP BY traj_id"
    kursor.execute(sql)
    trajs = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(trajs), outputs.VoortgangRegel.MODUS_NUM, 'Trajectories verwerkt')
    errors = 0
    for traj in trajs:
        sql = "SELECT id, node FROM rel_traj_tweets WHERE traj_id=%d" % (traj[0])
        kursor.execute(sql)
        tweets = kursor.fetchall()
        for i in range(len(tweets[:-1])):
            try:
                if tweets[i]!=tweets[i+1]:
                    sql= "INSERT INTO routes (traj_id, route_num, startrel, endrel, startnode, endnode) VALUES (%d, %d, %d, %d, %d, %d)" % (traj[0], i, tweets[i][0], tweets[i+1][0], tweets[i][1], tweets[i+1][1])
                    kursor.execute(sql)
            except TypeError:
                errors +=1
        monitor.plusEen()
    print ' Errors: %d' % errors
    kursor.connection.commit()
    kursor.close()

def calculateASTAR():
    dbtables.ROUTE_STEPS.createTable()
    kursor = dbconnect.Verbinding().kursor
    astarsql = "SELECT id, source, target, cost, reverse_cost, x1, y1, x2, y2 FROM ptrouting"
    sql = "SELECT id, startnode, endnode FROM routes"
    kursor.execute(sql)
    routes = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(routes), outputs.VoortgangRegel.MODUS_NUM, 'Routes verwerkt')
    errors = 0
    for route in routes:
        sql = "INSERT INTO route_steps (route_id, seq, node, edge, cost) " \
              "SELECT %d, seq, id1, id2, cost FROM pgr_astar('%s', %d, %d, true, true)" % (route[0], astarsql, route[1], route[2])
        try:
            kursor.execute(sql)
            #steps = kursor.fetchall()
            #for step in steps:
            #    sql = "INSERT INTO route_steps (route_id, seq, node, edge, cost) VALUES (%d, %d, %d, %d, %f)" % (route[0], step[0], step[1], step[2], step[3])
            #    kursor.execute(sql)
            kursor.connection.commit()
        except psycopg2.InternalError:
            errors += 1
            kursor.connection.commit()
            kursor.close()
            kursor = dbconnect.Verbinding().kursor
        monitor.plusEen()
    print 'Errors: %d' % errors
    kursor.connection.commit()
    kursor.close()

def calculateFrequency():
    con = dbconnect.Verbinding()
    print "\nFrequentietabel maken..."
    sql = "UPDATE ptrouting SET used=f.c FROM " \
          "(SELECT ptrouting.id i, count(route_steps.id) c FROM ptrouting " \
          "LEFT JOIN route_steps ON ptrouting.id=route_steps.edge " \
          "GROUP BY ptrouting.id) AS f " \
          "WHERE id=f.i"
    con.exe(sql)
    con.commit()
    con.sluit()

def createPtgeometry():
    con= dbconnect.Verbinding()
    sql = "ALTER TABLE ptrouting DROP COLUMN IF EXISTS geom_p, " \
          "ADD COLUMN geom_p geometry; " \
          "UPDATE ptrouting SET geom_p=geom_way; " \
          "SELECT ptrouting.id, src.geom, tgt.geom, src.lijnnaam, edge_type FROM ptrouting " \
          "INNER JOIN ptvertices AS srcv ON source=srcv.node_id " \
          "INNER JOIN ptvertices AS tgtv ON target=tgtv.node_id " \
          "INNER JOIN stops AS src ON srcv.stop_id=src.id " \
          "INNER JOIN stops AS tgt ON tgtv.stop_id=tgt.id " \
          "WHERE edge_type IN ('BOAT', 'BUS', 'TRAM', 'METRO', 'TRAIN')"
    routes = con.selectAll(sql)
    monitor = outputs.VoortgangRegel(len(routes), outputs.VoortgangRegel.MODUS_NUM, 'OV verwerkt')
    errors = 0
    for id, src, tgt, lijnnaam, type in routes:
        if type == 'TRAIN':
            sql = "SELECT g, ST_Line_Locate_Point(g, '%s'), ST_Line_Locate_Point(g, '%s') FROM " \
              "(SELECT ST_LineMerge(geom) g FROM ptnet_nl WHERE transportm='TRAIN') AS l " \
              "ORDER BY (ST_Distance(g, '%s')+ST_Distance(g, '%s')) ASC LIMIT 1" \
              % (src, tgt, src, tgt)
        else:
            sql = "SELECT g, ST_Line_Locate_Point(g, '%s'), ST_Line_Locate_Point(g, '%s') FROM " \
                  "(SELECT ST_LineMerge(geom) g FROM ptnet_nl WHERE publiccode='%s') AS l " \
                  "ORDER BY (ST_Distance(g, '%s')+ST_Distance(g, '%s')) ASC LIMIT 1" \
                  % (src, tgt, lijnnaam, src, tgt)
        netroutes = con.selectOne(sql)
        if netroutes is None:
            if type != 'TRAIN':
                sql = "SELECT g, ST_Line_Locate_Point(g, '%s'), ST_Line_Locate_Point(g, '%s') FROM " \
                      "(SELECT ST_LineMerge(geom) g FROM ptnet WHERE ST_DWithin('%s', geom, 0.01) OR ST_DWithin('%s', geom, 0.01)) AS l " \
                      "ORDER BY (ST_Distance(g, '%s')+ST_Distance(g, '%s')) ASC LIMIT 1" \
                      % (src, tgt, src, tgt, src, tgt)
                netroutes = con.selectOne(sql)
            errors += 1
        route, srcfrac, tgtfrac = netroutes
        if srcfrac < tgtfrac:
            sql = "UPDATE ptrouting SET geom_p=ST_Line_Substring('%s', %f, %f) WHERE id=%d" % (route, srcfrac, tgtfrac, id)
        elif tgtfrac < srcfrac:
            sql = "UPDATE ptrouting SET geom_p=ST_Reverse(ST_Line_Substring('%s', %f, %f)) WHERE id=%d" % (route, tgtfrac, srcfrac, id)
        else:
            sql = "UPDATE ptrouting SET geom_p=geom_way WHERE id=%d" % (id)
        con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()
    print 'errors: ', errors
