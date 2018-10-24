#!/usr/bin/env python

'''
Builds a HySDS event product from the EONET event feed

'''

import os
import re
import json
import math
import shutil
import datetime
import pytz

from hysds.celery import app
from hysds.dataset_ingest import ingest
import hysds.orchestrator

VERSION = 'v1.0'
PRODUCT_PREFIX = 'event'

def build(event, submit):
    '''builds a HySDS product from the input event json. input is the usgs event. if submit
     is true, it submits the product directly'''
    ds = build_dataset(event)
    met = build_met(event)
    build_product_dir(ds, met)
    if submit:
        submit_product(ds, met)
    print('Publishing Event ID: {0}'.format(ds['label']))
    print('    event:        {0}'.format(event['title']))
    print('    source:       {0}'.format(event['sources'][0]['id']))
    print('    event time:   {0}'.format(ds['starttime']))
    print('    location:     {0}'.format(event['geometries'][-1]['coordinates'].reverse()))
    print('    version:      {0}'.format(ds['version']))

def build_id(event):
    try:
        source = event['sources'][0]['id']
        event_id = event['id']
        category = event['categories'][0]['title'].lower()
        stripped_dt = re.sub('-|:', '', event['geometries'][-1]['date'])
        uid = '{0}_{1}_{2}_{3}_{4}'.format(PRODUCT_PREFIX, category, source, event_id, stripped_dt)
    except:
        raise Exception('failed on {}'.format(event))
    return uid

def build_dataset(event):
    '''parse out the relevant dataset parameters and return as dict'''
    time = event['geometries'][-1]['date']
    #if type is point, build it into a polygon, otherwise use the polygon
    if event['geometries'][-1]['type'] == 'Point':
        location = build_polygon_geojson(event)
    else:
        location = event['geometries'][-1]
        del location['date']
    label = build_id(event)
    version = VERSION
    ds = {'label':label, 'starttime':time, 'endtime':time, 'location':location, 'version':version}
    return ds

def build_met(event):
    met = event
    return met

def convert_epoch_time_to_utc(epoch_timestring):
    dt = datetime.datetime.utcfromtimestamp(epoch_timestring).replace(tzinfo=pytz.UTC)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] # use microseconds and convert to milli

def build_point_geojson(event):
    lat = float(event['geometry']['coordinates'][1])
    lon = float(event['geometry']['coordinates'][0])
    return {'type':'point', 'coordinates': [lon, lat]}

def shift(lat, lon, bearing, distance):
    R = 6378.1  # Radius of the Earth
    bearing = math.pi * bearing / 180  # convert degrees to radians
    lat1 = math.radians(lat)  # Current lat point converted to radians
    lon1 = math.radians(lon)  # Current long point converted to radians
    lat2 = math.asin(math.sin(lat1) * math.cos(distance / R) +
                     math.cos(lat1) * math.sin(distance / R) * math.cos(bearing))
    lon2 = lon1 + math.atan2(math.sin(bearing) * math.sin(distance / R) * math.cos(lat1),
                             math.cos(distance / R) - math.sin(lat1) * math.sin(lat2))
    lat2 = math.degrees(lat2)
    lon2 = math.degrees(lon2)
    return [lon2, lat2]

def build_polygon_geojson(event):
    lat = float(event['geometries'][-1]['coordinates'][1])
    lon = float(event['geometries'][-1]['coordinates'][0])
    radius = 2.0
    l = range(0, 361, 20)
    coordinates = []
    for b in l:
        coords = shift(lat, lon, b, radius)
        coordinates.append(coords)
    return {"coordinates": [coordinates], "type": "polygon"}

def build_product_dir(ds, met):
    label = ds['label']
    ds_dir = os.path.join(os.getcwd(), label)
    ds_path = os.path.join(ds_dir, '{0}.dataset.json'.format(label))
    met_path = os.path.join(ds_dir, '{0}.met.json'.format(label))
    if not os.path.exists(ds_dir):
        os.mkdir(ds_dir)
    with open(ds_path, 'w') as outfile:
        json.dump(ds, outfile)
    with open(met_path, 'w') as outfile:
        json.dump(met, outfile)

def submit_product(ds, met):
    uid = ds['label']
    ds_dir = os.path.join(os.getcwd(), uid)
    try:
        ingest(uid, './datasets.json', app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, ds_dir, None) 
        if os.path.exists(uid):
            shutil.rmtree(uid)
    except Exception, err:
        print('failed on submission of {0} with {1}'.format(uid, err))

