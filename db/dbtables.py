__author__ = 'gijspeters'


class DbTable:

    def __init__(self, naam, sql):
        self.naam = naam
        self.sql = sql

    def getNaam(self):
        return self.naam

    def getSql(self):
        return self.sql


TEST_TABEL = DbTable('test_tabel', 'CREATE TABLE test_tabel (id serial NOT NULL )')

TWEETS_AMSTERDAM = DbTable('tweets_amsterdam',
                            'CREATE TABLE tweets_amsterdam ( id serial NOT NULL, '
                            'tweet_id character varying(50), tweet_name character varying(150), '
                            'tijddatum timestamp without time zone, the_geom geometry, '
                            'lat double precision, lon double precision, datum date, '
                            'CONSTRAINT tweets_amsterdam_pkey PRIMARY KEY (id)) WITH (OIDS=FALSE); '
                            'ALTER TABLE tweets_amsterdam OWNER TO postgres;')

USERS = DbTable('users', "CREATE TABLE public.users ( id serial, name character varying(150), "
                         "days integer, tweets integer, useless boolean DEFAULT false, processed boolean DEFAULT false, "
                         "CONSTRAINT uid PRIMARY KEY (id)) WITH ( OIDS = FALSE);")

SCRAPED_TWEETS = DbTable('scraped_tweets',
                            'CREATE TABLE scraped_tweets ( id serial NOT NULL, '
                            'tweet_id character varying(50), tweet_name character varying(150), '
                            'tijddatum timestamp without time zone, the_geom geometry, '
                            'lat double precision, lon double precision, datum date, '
                            'geom_3857 geometry, '
                            'CONSTRAINT scraped_tweets_pkey PRIMARY KEY (id)) WITH (OIDS=FALSE); '
                            'ALTER TABLE scraped_tweets OWNER TO postgres;')

TRAJECTORIES =  DbTable('trajectories', 'CREATE TABLE trajectories '
                                        '(  id serial NOT NULL, '
                                          'name character varying(150), '
                                          'starttime timestamp without time zone, '
                                          'endtime timestamp without time zone, '
                                          'lijn geometry, route geometry, '
                                          'CONSTRAINT trajectories_pkey PRIMARY KEY (id) '
                                        ') WITH ( OIDS=FALSE );'
                                        'ALTER TABLE trajectories OWNER TO postgres;')

REL_TRAJ_TWEETS = DbTable('rel_traj_tweets', 'CREATE TABLE public.rel_traj_tweets'
                                                '(id serial, traj_id integer, tweet_id integer, '
                                                'PRIMARY KEY (id), '
                                                'FOREIGN KEY (traj_id) REFERENCES trajectories (id) ON UPDATE NO ACTION ON DELETE NO ACTION, '
                                                'FOREIGN KEY (tweet_id) REFERENCES scraped_tweets (id) ON UPDATE NO ACTION ON DELETE NO ACTION , edge integer) WITH (OIDS = FALSE) TABLESPACE pg_default;'
                                                'ALTER TABLE trajectories OWNER TO postgres;')

ROUTES = DbTable('routes', 'CREATE TABLE routes ( id serial NOT NULL, traj_id integer, route_num integer, startrel integer, endrel integer, startnode integer, '
                           'endnode integer, lijn geometry, path geometry, cost double precision, realtime double precision, useful boolean, isfast boolean, '
                           'otpduration double precision, otpsurplus double precision, otpspperc double precision, stationaryedge integer, '
                           'CONSTRAINT routes_pkey PRIMARY KEY (id)) '
                           'WITH (OIDS=FALSE); ALTER TABLE routes OWNER TO postgres;')

ROUTE_STEPS = DbTable('route_steps', 'CREATE TABLE route_steps (id serial NOT NULL, route_id integer, seq integer, node integer, edge integer, cost double precision, '
                                     'CONSTRAINT route_steps_pkey PRIMARY KEY (id) ) '
                                     'WITH (OIDS=FALSE); ALTER TABLE route_steps OWNER TO postgres;')

VERTICES = DbTable('vertices', 'CREATE TABLE vertices (id serial NOT NULL, node_id integer, lon double precision, lat double precision, geom geometry, type integer, '
                                     'CONSTRAINT vertices_pkey PRIMARY KEY (id) ) '
                                     'WITH (OIDS=FALSE); ALTER TABLE vertices OWNER TO postgres;')

STOPS = DbTable('stops', 'CREATE TABLE stops (id serial NOT NULL, pointref integer, geom geometry, lijnnaam character varying, '
                         'CONSTRAINT stops_pkey PRIMARY KEY (id) )'
                         'WITH (OIDS=FALSE); ALTER TABLE stops OWNER TO postgres;')

STATIONS = DbTable('stations', 'CREATE TABLE stations (id serial NOT NULL, pointref integer, geom geometry, '
                               'CONSTRAINT stations_pkey PRIMARY KEY (id) )'
                               'WITH (OIDS=FALSE); ALTER TABLE stations OWNER TO postgres;')

PTVERTICES = DbTable('ptvertices', 'CREATE TABLE ptvertices (id serial NOT NULL, node_id integer, lon double precision, lat double precision, geom geometry, '
                                   'node_type character varying (20), station_id integer, stop_id integer, '
                                     'CONSTRAINT ptvertices_pkey PRIMARY KEY (id) ) '
                                     'WITH (OIDS=FALSE); ALTER TABLE ptvertices OWNER TO postgres;')

LIJNEN = DbTable('lijnen', 'CREATE TABLE lijnen (id serial NOT NULL, lijnnaam character varying (20), '
                           'type character varying (20), '
                           'CONSTRAINT lijnen_pkey PRIMARY KEY (id) )'
                           'WITH (OIDS=FALSE); ALTER TABLE lijnen OWNER TO postgres;')

OTPFAST = DbTable('otpfast', 'CREATE TABLE otpfast (id serial NOT NULL, route_id integer, duration integer, tooslow boolean, '
                             'CONSTRAINT otpfast_pkey PRIMARY KEY (id) )'
                             'WITH (OIDS=FALSE); ALTER TABLE otpfast OWNER TO postgres;')

OTPPREF = DbTable('otppref', 'CREATE TABLE otppref (id serial NOT NULL, route_id integer, duration integer, tooslow boolean,'
                             'CONSTRAINT otppref_pkey PRIMARY KEY (id) )'
                             'WITH (OIDS=FALSE); ALTER TABLE otppref OWNER TO postgres;')

OTPLEGS = DbTable('otplegs', 'CREATE TABLE otplegs (id serial NOT NULL, route_id integer, legnum integer, points geometry, '
                             'mode character varying, duration integer, distance double precision, timesurplus double precision, spperc double precision, '
                             'CONSTRAINT otplegs_pkey PRIMARY KEY (id) )'
                             'WITH (OIDS=FALSE); ALTER TABLE otplegs OWNER TO postgres;')

OTPEDGES = DbTable('otpedges', 'CREATE TABLE otpedges (id serial NOT NULL, leg_id integer, edgenum integer, geom geometry, mode character varying, network_id integer, '
                               'CONSTRAINT otpedges_pkey PRIMARY KEY (id) ) '
                               'WITH (OIDS=FALSE); ALTER TABLE otpedges OWNER TO postgres;')

OTPNETWORK = DbTable('otpnetwork', 'CREATE TABLE otpnetwork (id serial NOT NULL, geom geometry, mode character varying, used integer, '
                                   'used_s integer, touristtime double precision, touristtime_s double precision, used_r integer, '
                                   'touristtime_r double precision, thm_r double precision, thm_s double precision, thm double precision, '
                                   'avgth double precision, avgth_r double precision, avgth_s double precision, '
                                   'CONSTRAINT otpnetwork_pkey PRIMARY KEY (id) ) '
                                   'WITH (OIDS=FALSE); ALTER TABLE otpnetwork OWNER TO postgres;')

OTPROUTES = DbTable('otproutes', 'CREATE TABLE otproutes ( id serial NOT NULL, traj_id integer, route_num integer, startrel integer, endrel integer, starttweet integer, '
                           'endtweet integer, path geometry, realtime integer, useful boolean, isfast boolean, '
                           'otpduration double precision, otpsurplus double precision, otpspperc double precision, stationaryedge integer, '
                           'CONSTRAINT otproutes_pkey PRIMARY KEY (id)) '
                           'WITH (OIDS=FALSE); ALTER TABLE otproutes OWNER TO postgres;')

PRISMEDGES = DbTable('prismedges', 'CREATE TABLE prismedges (route_id integer, edge_id integer, '
                                   'db integer, df integer, '
                                   'CONSTRAINT prismedges_pkey PRIMARY KEY (route_id, edge_id) ) '
                                   'WITH (OIDS=FALSE); ALTER TABLE prismedges OWNER TO postgres;')

PROBEDGES = DbTable('probedges', 'CREATE TABLE probedges (id serial NOT NULL, route_id integer, edge_id integer, '
                                   'db integer, df integer, x double precision, y double precision, P double precision, E double precision, '
                                   'CONSTRAINT probedges_pkey PRIMARY KEY (route_id, edge_id) ) '
                                   'WITH (OIDS=FALSE); ALTER TABLE probedges OWNER TO postgres;')

HVERTICES = DbTable('hvertices', 'CREATE TABLE hvertices (id serial NOT NULL, vertex_id integer, label character varying, geom geometry, '
                                 'CONSTRAINT hvertices_pkey PRIMARY KEY (id) ) '
                                 'WITH (OIDS=FALSE); ALTER TABLE hvertices OWNER TO postgres;')

HEDGES = DbTable('hedges', 'CREATE TABLE hedges (id serial NOT NULL, edge_id integer, label character varying, from_id integer, to_id integer, geom geometry, distance real, midpoint geometry, '
                           'CONSTRAINT hedges_pkey PRIMARY KEY (id) ) '
                           'WITH (OIDS=FALSE); ALTER TABLE hedges OWNER TO postgres;')

HROUTES = DbTable('hroutes', 'CREATE TABLE hroutes (route_id integer, ti integer, tj integer, Dij integer, Dji integer, '
                             'CONSTRAINT hroutes_pkey PRIMARY KEY (route_id) )')

EDGE_ID_TMP_TABLE = DbTable('edge_id_temp_table', 'CREATE TABLE edge_id_temp_table (id integer, CONSTRAINT tmp_pkey id)')