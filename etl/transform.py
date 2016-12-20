__author__ = 'gijspeters'

from db import dbconnect
from misc.outputs import VoortgangRegel

def createPoints(veld, srid):
    kursor = dbconnect.Verbinding().kursor
    sql = "SELECT id, lat, lon FROM scraped_tweets"
    kursor.execute(sql)
    coords = kursor.fetchall()
    monitor = VoortgangRegel(len(coords), VoortgangRegel.MODUS_NUM, 'Regels verwerkt' )
    monitor.nieuweRegel()
    for coord in coords:
        sql = "UPDATE scraped_tweets SET %s=ST_Transform(ST_SetSRID(ST_MakePoint(%f, %f), 4326), %d) WHERE id=%d" % (veld, coord[2], coord[1], srid, coord[0])
        kursor.execute(sql)
        monitor.plusEen()
    kursor.connection.commit()
    kursor.close()