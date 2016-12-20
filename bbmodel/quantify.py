__author__ = 'gijspeters'

from db import dbconnect
from misc.outputs import VoortgangRegel

def calcCostPaths():
    print 'Kosten berekenen...'
    con = dbconnect.Verbinding()
    sql = "UPDATE routes SET path=r.g, cost=r.c FROM " \
          "(SELECT routes.id i, steps.g g, steps.c c FROM routes " \
          "INNER JOIN (SELECT route_id, ST_MakeLine(ptrouting.geom_p) g, sum(route_steps.cost) c FROM route_steps " \
          "INNER JOIN ptrouting ON edge=ptrouting.id WHERE edge<>-1 GROUP BY route_id) AS steps ON routes.id=steps.route_id " \
          ") AS r WHERE routes.id=r.i;"
    con.exe(sql)
    con.commit()
    con.sluit()

def calcRealTimes():
    print 'Tijden berekenen...'
    con = dbconnect.Verbinding()
    sql = "UPDATE routes SET realtime=r.r FROM" \
          "(SELECT routes.id i, abs(extract(epoch FROM tgtt.tijddatum-srct.tijddatum)) r FROM routes " \
          "INNER JOIN rel_traj_tweets AS src ON routes.startrel=src.id " \
          "INNER JOIN rel_traj_tweets AS tgt ON routes.endrel=tgt.id " \
          "INNER JOIN scraped_tweets AS srct ON src.tweet_id=srct.id " \
          "INNER JOIN scraped_tweets AS tgtt ON tgt.tweet_id=tgtt.id " \
          ") AS r WHERE routes.id=r.i"
    con.exe(sql)
    con.commit()
    con.sluit()

def calcTimeSurplus():
    print 'Tijdsurplus berekenen...'
    con = dbconnect.Verbinding()
    sql = "UPDATE routes SET otpduration=d, otpsurplus=(r.realtime)-d " \
          "FROM routes AS r INNER JOIN " \
          "(SELECT route_id, sum(duration) d FROM otplegs GROUP BY route_id) AS l " \
          "ON r.id=l.route_id " \
          "WHERE routes.id=r.id"
    con.exe(sql)
    sql = "SELECT id, route_id, legnum, mode, duration, distance FROM otplegs"
    legs = con.selectAll(sql)
    sql = "SELECT id, isfast, otpsurplus FROM routes"
    routes = con.selectAll(sql)
    monitor = VoortgangRegel(len(routes), VoortgangRegel.MODUS_NUM, 'Routes verwerkt')
    # arrays maken
    ids = []
    routeids = []
    legnums = []
    modes = []
    durations = []
    distances = []
    for leg in legs:
        ids.append(leg[0])
        routeids.append(leg[1])
        legnums.append(leg[2])
        modes.append(leg[3])
        durations.append(leg[4])
        distances.append(leg[5])
    #routes verwerken
    for id, isfast, otpsurplus in routes:
        legids = [i for i, x in enumerate(routeids) if x == id]
        if isfast:
            routelegnums = [legnums[i] for i in legids]
            maxlegnumid = legnums.index(max(routelegnums))
            for i in legids:
                if legnums[i] == 0 or legnums[i] == legnums[maxlegnumid]:
                    surplus = otpsurplus * (distances[i] / (distances[0] + distances[maxlegnumid]))
                else:
                    surplus = 0
                tijd = durations[i] + surplus
                sql = "UPDATE otplegs SET timesurplus=%f, realtime=%f WHERE id=%d" % (surplus, tijd, i)
                con.exe(sql)
        else:
            walkdistance = sum([distances[j] for j in [i for i, x in enumerate(modes) if x == 'WALK']])
            for i in legids:
                if modes[i] == 'WALK':
                    surplus = otpsurplus * (durations[i] / walkdistance)
                else:
                    surplus = 0
                tijd = durations[i] + surplus
                sql = "UPDATE otplegs SET timesurplus=%f, realtime=%f WHERE id=%d" % (surplus, tijd, i)
                con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()

def calcEdgeTime():
    con = dbconnect.Verbinding()
    sql = "SELECT otpedges.id, ST_Length(ST_Transform(otpedges.geom, 28992)), ST_Length(ST_Transform(e.g, 28992)), otplegs.realtime, e.c FROM otpedges " \
          "INNER JOIN (SELECT leg_id, ST_MakeLine(geom) g, count(id) c FROM otpedges GROUP BY leg_id) AS e ON otpedges.leg_id=e.leg_id " \
          "INNER JOIN otplegs ON otpedges.leg_id=otplegs.id "
    edges = con.selectAll(sql)
    monitor = VoortgangRegel(len(edges), VoortgangRegel.MODUS_NUM, 'Edges verwerkt')
    for id, distance, totaldistance, totaltime, edgecount in edges:
        try:
            if totaldistance == 0:
                realtime = totaltime * (1 / edgecount)
            else:
                realtime = totaltime * (distance / totaldistance)
        except Exception, e:
            continue
        sql = "UPDATE otpedges SET realtime=%f WHERE id=%d" % (realtime, id)
        con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()

def calcEdgeLength():
    print 'Lengtes uitrekenen...'
    con = dbconnect.Verbinding()
    sql = "UPDATE otpnetwork SET length=ST_Length(ST_Transform(n.geom, 28992)) " \
          "FROM otpnetwork AS n WHERE otpnetwork.id=n.id"
    con.exe(sql)
    con.commit()
    con.sluit()

def calcStationaryRouteEdges():
    con = dbconnect.Verbinding()
    sql = "SELECT routes.id, the_geom FROM routes " \
          "INNER JOIN rel_traj_tweets ON routes.endrel=rel_traj_tweets.id " \
          "INNER JOIN scraped_tweets ON rel_traj_tweets.tweet_id=scraped_tweets.id " \
          "WHERE startnode=endnode AND otpduration IS NULL"
    routes = con.selectAll(sql)
    monitor = VoortgangRegel(len(routes), VoortgangRegel.MODUS_NUM, 'Routes verwerkt')
    for id, geom in routes:
        sql = "UPDATE routes SET stationaryedge=nn.id, isstationary=TRUE FROM " \
              "(SELECT id FROM (SELECT id, geom FROM otpnetwork WHERE ST_DWithin('%s', geom, 0.0007) AND mode='WALK') AS n " \
              "ORDER BY ST_Distance('%s', n.geom) ASC LIMIT 1) AS nn " \
              "WHERE routes.id=%d" % (geom, geom, id)
        con.exe(sql)
        monitor.plusEen()
    con.commit()
    con.sluit()

def calcTouristTime():
    #route times
    print 'Routetijden optellen...'
    con = dbconnect.Verbinding()
    sql = "UPDATE otpnetwork SET touristtime_r=n.t FROM " \
          "(SELECT network_id, sum(realtime) t FROM otpedges GROUP BY network_id) AS n " \
          "WHERE id=network_id "
    con.exe(sql)
    con.commit()
    #stationary times
    print 'Stationaire tijden optellen...'
    sql = "UPDATE otpnetwork SET touristtime_s=r.t FROM " \
          "(SELECT stationaryedge, sum(realtime) t FROM routes WHERE stationaryedge IS NOT NULL GROUP BY stationaryedge) AS r " \
          "WHERE id=stationaryedge; " \
          "UPDATE otpnetwork SET used_s=r.u FROM " \
          "(SELECT stationaryedge, count(DISTINCT name) u FROM routes " \
          "INNER JOIN trajectories ON routes.traj_id=trajectories.id GROUP BY stationaryedge) AS r " \
          "WHERE otpnetwork.id=stationaryedge; " \
          "UPDATE otpnetwork SET used_s=0, touristtime_s=0 WHERE used_s IS NULL"
    con.exe(sql)
    con.commit()
    #total times
    print 'Totale tijden uitrekenen...'
    sql = "UPDATE otpnetwork SET touristtime=n.touristtime_r+n.touristtime_s FROM " \
          "otpnetwork AS n " \
          "WHERE otpnetwork.id=n.id "
    con.commit()
    con.exe(sql)
    con.sluit()

def calcDependentVars():
    print 'Toeristuren/meter uitrekenen...'
    con = dbconnect.Verbinding()
    sql = "UPDATE otpnetwork AS n SET " \
          "thm_r=nn.touristtime_r/ST_Length(ST_Transform(nn.geom, 28992))/3600, " \
          "thm_s=nn.touristtime_s/ST_Length(ST_Transform(nn.geom, 28992))/3600 " \
          "FROM otpnetwork AS nn " \
          "WHERE n.id=nn.id AND ST_Length(ST_Transform(n.geom, 28992))>0; " \
          "UPDATE otpnetwork AS n SET " \
          "thm=nn.thm_r+nn.thm_s " \
          "FROM otpnetwork AS nn " \
          "WHERE n.id=nn.id"
    con.exe(sql)
    con.commit()
    print 'Gemiddelde verblijftijd stationair uitrekenen...'
    sql = "UPDATE otpnetwork AS n SET " \
          "avgth_s=nn.touristtime_s/nn.used_s " \
          "FROM otpnetwork AS nn " \
          "WHERE n.id=nn.id AND n.used_s>0"
    con.exe(sql)
    con.commit()
    print 'Gemiddelde verblijftijden uitrekenen...'
    sql = "UPDATE otpnetwork AS n SET " \
          "avgth_r=nn.touristtime_r/nn.used_r, " \
          "avgth=nn.touristtime/nn.used " \
          "FROM otpnetwork AS nn " \
          "WHERE n.id=nn.id"
    con.exe(sql)
    con.commit()
    con.sluit()