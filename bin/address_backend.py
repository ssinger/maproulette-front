#!/usr/bin/env python

"""
This program is a demo implementation of a challenge generation script
for maproulette.

This module connects to a OSM Osmosis database and finds restaraunts with
no address near Toronto. It then adds them to challenge 1


"""

import geojson


import psycopg2

source_db= 'host=localhost user=maproulette dbname=osm'
challenge_db = 'host=localhost user=maproulette dbname = maproulette'

source_conn = psycopg2.connect(source_db)
challenge_conn  = psycopg2.connect(challenge_db)

def load_points():
    """
    Performs a query against OSM to get POI's with the addresses.
    These POI's are then loaded into the system as tasks.
    """
    cur = source_conn.cursor()
    challenge_cur = challenge_conn.cursor()
    
    # create a temp table to store the ids we have seen so far
    challenge_cur.execute(" create temp table active_ids(external_id text);")
    
    # get the geometry as both JSON and raw because we
    # will use the JSON for building the render json
    # and use the raw data for the centroid
    cur.execute(" select id,version,ST_AsGeoJSON(geom) as geom,            "
                "      geom as geom_raw from nodes                         "
                "      where tags->'amenity' = 'restaurant' and            " 
                "            not tags?'addr:street' and                    "
                "            geom && ST_Buffer(ST_GeomFromText             "
                "                             ('POINT(-79.38238 43.6553)')," 
                "                               1.0::float)                "
                )

    for place in cur.fetchall():
        task_ext_id = "%s_%s" % ( place[0], place[1])
        challenge_cur.execute("insert into active_ids (external_id) values(%s)"
                              , [ task_ext_id])
        # 
        # Does this task exist?
        challenge_cur.execute("select task_id from task where external_id=%s "
                              " and challenge_id=%s limit 1"
                              ,[task_ext_id,1])
        if challenge_cur.fetchone() == None:
            # we must add this row.
            challenge_cur.execute("insert into task (challenge_id "
                                  "                  ,state_id "
                                  "                  , render_geometry "
                                  "                  , external_id "
                                  "                  , centroid    "
                                  "                  ) values ( %s"
                                  "                  , %s "
                                  "                  , %s "
                                  "                  , %s "
                                  "                  , %s )"
                                  ,[1 , 1 , place[2],task_ext_id,place[3]])
    #
    # mark any ids for this challenge that are active as expired
    # if they no longer show up in the OSM database query
    challenge_cur.execute("update task set state_id=2 where "
                          " challenge_id=1                  "
                          " and state_id=1                  "
                          " and external_id not in          "
                          "       (select * FROM active_ids)"
                          )
    challenge_cur.close()
    cur.close()
    challenge_conn.commit()
    source_conn.rollback()


if __name__ == '__main__':
    load_points()
