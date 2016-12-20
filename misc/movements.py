__author__ = 'gijspeters'

import outputs
from db import dbconnect


def createLines():
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT id FROM trajectories"
    kursor.execute(sql)
    ids = kursor.fetchall()
    monitor = outputs.VoortgangRegel(len(ids), outputs.VoortgangRegel.MODUS_NUM, 'Trajectories verwerkt')
    for id in ids:
        sql = "UPDATE trajectories SET lijn=traj.lijn FROM (SELECT ST_MakeLine(tw.geom_3857) AS lijn " \
              "FROM (SELECT scraped_tweets.geom_3857 FROM rel_traj_tweets INNER JOIN " \
              "scraped_tweets ON rel_traj_tweets.tweet_id=scraped_tweets.id WHERE rel_traj_tweets.traj_id=%d) AS tw) AS traj WHERE id=%d" % (id[0], id[0])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    kursor.close()
