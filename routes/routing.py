__author__ = 'gijspeters'

import dbconnect
import psycopg2

from db import dbtables
from misc import outputs


def nearestNodes():
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT rel_traj_tweets.id, the_geom FROM scraped_tweets INNER JOIN rel_traj_tweets ON scraped_tweets.id=rel_traj_tweets.tweet_id ORDER BY traj_id ASC"
    kursor.execute(sql)
    tweets = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(tweets), outputs.VoortgangRegel.MODUS_NUM, 'Tweets verwerkt')
    i = 0
    for tweet in tweets:
        sql = "UPDATE rel_traj_tweets SET node=nodes.node_id FROM " \
              "(SELECT subset.node_id FROM (SELECT node_id, geom FROM vertices WHERE ST_DWithin('%s', geom, 0.01)) AS subset " \
              "ORDER BY ST_Distance(subset.geom, '%s') ASC LIMIT 1) AS nodes " \
              "WHERE rel_traj_tweets.id=%d" % (tweet[1], tweet[1], tweet[0])
        kursor.execute(sql)
        monitor.plusEen()
        i+=1
        if i>100:break
    kursor.connection.commit()
    kursor.close()

def createVertexTable():
    dbtables.VERTICES.createTable()
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT source, max(x1), max(y1) FROM routing GROUP BY source"
    kursor.execute(sql)
    sources = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(sources), outputs.VoortgangRegel.MODUS_NUM, 'Sources verwerkt')
    for source in sources:
        sql = "INSERT INTO vertices (node_id, lon, lat, geom) VALUES (%d, %f, %f, ST_SetSRID(ST_MakePoint(%f, %f), 4326))" % (source[0], source[1], source[2], source[1], source[2])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    sql = "SELECT target, max(x2), max(y2) FROM routing LEFT JOIN vertices ON routing.target=vertices.node_id WHERE vertices.node_id IS NULL GROUP BY target"
    kursor.execute(sql)
    targets = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(targets), outputs.VoortgangRegel.MODUS_NUM, 'Targets verwerkt')
    for target in targets:
        sql = "INSERT INTO vertices (node_id, lon, lat, geom) VALUES (%d, %f, %f, ST_SetSRID(ST_MakePoint(%f, %f), 4326))" % (target[0], target[1], target[2], target[1], target[2])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    kursor.close()

def createRouteTable():
    dbtables.ROUTES.createTable()
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT id, traj_id FROM rel_traj_tweets WHERE node IS NOT NULL"
    kursor.execute(sql)
    trajs = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(trajs), outputs.VoortgangRegel.MODUS_NUM, 'Trajectories verwerkt')
    errors = 0
    for traj in trajs:
        sql = "SELECT traj.node FROM (SELECT node, tweet_id FROM rel_traj_tweets WHERE traj_id=%d) AS traj ORDER BY traj.tweet_id DESC" % (traj[1])
        kursor.execute(sql)
        tweets = kursor.fetchall()
        for i in range(len(tweets[:-1])):
            try:
                if tweets[i]!=tweets[i+1]:
                    sql= "INSERT INTO routes (rel_id, traj_id, route_num, startnode, endnode) VALUES (%d, %d, %d, %d, %d)" % (traj[0], traj[1], i, tweets[i][0], tweets[i+1][0])
                    kursor.execute(sql)
            except TypeError:
                errors +=1
        monitor.plusEen()
    print 'Errors: %d' % errors
    kursor.connection.commit()
    kursor.close()

def calculateASTAR():
    dbtables.ROUTE_STEPS.createTable()
    kursor = dbconnect.Verbinding().kursor
    astarsql = "SELECT id, source, target, cost, x1, y1, x2, y2 FROM routing"
    sql = "SELECT id, startnode, endnode FROM routes"
    kursor.execute(sql)
    routes = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(routes), outputs.VoortgangRegel.MODUS_NUM, 'Routes verwerkt')
    errors = 0
    for route in routes:
        sql = "SELECT seq, id1, id2, cost FROM pgr_astar('%s', %d, %d, false, false)" % (astarsql, route[1], route[2])
        try:
            kursor.execute(sql)
            steps = kursor.fetchall()
            for step in steps:
                sql = "INSERT INTO route_steps (route_id, seq, node, edge, cost) VALUES (%d, %d, %d, %d, %f)" % (route[0], step[0], step[1], step[2], step[3])
                kursor.execute(sql)
                kursor.connection.commit()
        except psycopg2.InternalError:
            errors += 1
            kursor.close()
            kursor = dbconnect.Verbinding().kursor
        monitor.plusEen()
    print 'Errors: %d' % errors
    kursor.connection.commit()
    kursor.close()

def makeFrequencyColumn():
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT count(edge), edge FROM route_steps WHERE edge > 0 GROUP BY edge"
    kursor.execute(sql)
    edges = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(edges), outputs.VoortgangRegel.MODUS_NUM, 'Edges verwerkt')
    for edge in edges:
        sql = "UPDATE routing SET used=%d WHERE id=%d" % edge
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    kursor.close()
