#__author__ = 'gijspeters'


class TwitObject:
    def __init__(self, id, naam, tijd, geom, lat, lon, datum):
        self.id = id
        self.naam = naam
        self.tijd = tijd
        self.geom = geom
        self.lat = lat
        self.lon = lon
        self.datum = datum

    def getData(self):
        return (self.id, self.naam, self.tijd, self.geom, self.lat, self.lon, self.datum)


def createFromFetch(fetch):
    id, naam, tijd, geom, lat, lon, datum = fetch
    return TwitObject(id, naam, tijd, geom, lat, lon, datum)