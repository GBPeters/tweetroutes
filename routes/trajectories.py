__author__ = 'gijspeters'

import datetime
import dbconnect

from db import dbtables
from misc import outputs

THRESHOLD = 2

def createTrajectories():
    dbtables.TRAJECTORIES.createTable()
    dbtables.REL_TRAJ_TWEETS.createTable()
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT DISTINCT tweet_name FROM scraped_tweets"
    kursor.execute(sql)
    namen = kursor.fetchall()
    monitor = outputs.TrajectoryMonitor(len(namen)).nieuweMonitor()
    for naam in namen:
        sql = "SELECT id, tijddatum FROM scraped_tweets WHERE tweet_name='%s' ORDER BY tijddatum ASC" % naam[0]
        kursor.execute(sql)
        tweets = kursor.fetchall()
        trajectory = [tweets[0]]
        for tweet in tweets[1:]:
            oudetijd = trajectory[-1][1]
            nieuwetijd = tweet[1]
            thresholdtijd = oudetijd + datetime.timedelta(hours=THRESHOLD)
            if nieuwetijd > thresholdtijd:
                if len(trajectory)>1:
                    writeTrajectory(trajectory, naam[0])
                    monitor.updateRegel(naam, trajectory[0][1], trajectory[-1][1])
                trajectory = [tweet]
            else:
                trajectory.append(tweet)
        if len(trajectory)>1:
            writeTrajectory(trajectory, naam[0])
            monitor.updateRegel(naam, trajectory[0][1], trajectory[-1][1])
    kursor.close()

def writeTrajectory(trajectory, naam):
    kursor = dbconnect.Verbinding().kursor
    sql = "INSERT INTO trajectories (name, starttime, endtime) VALUES ('%s', '%s', '%s') RETURNING id" % (naam, trajectory[0][1], trajectory[-1][1])
    kursor.execute(sql)
    id = kursor.fetchone()[0]
    for t in trajectory:
        sql = "INSERT INTO rel_traj_tweets (traj_id, tweet_id) VALUES (%d, %d)" % (id, t[0])
        kursor.execute(sql)
    kursor.connection.commit()
    kursor.close()