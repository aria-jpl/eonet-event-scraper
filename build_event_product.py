#!/usr/bin/env python

"""
Builds a HySDS event product from the EONET event feed

"""

import os
import re
import json
import math
import datetime
import pytz

VERSION = 'v1.0'
PRODUCT_PREFIX = 'event'


def build_hysds_product(event):
    """builds a HySDS product from the input event json. input is the USGS event. if submit
     is true, it submits the product directly"""
    dataset = build_dataset(event)
    metadata = build_metadata(event)
    build_product_dir(dataset, metadata)
    print('Publishing Event ID: {0}'.format(dataset['label']))
    print('    event:        {0}'.format(event['title']))
    print('    source:       {0}'.format(event['sources'][0]['id']))
    print('    event time:   {0}'.format(dataset['starttime']))
    print('    location:     {0}'.format(event['geometries'][-1]['coordinates'].reverse()))
    print('    version:      {0}'.format(dataset['version']))


def build_id(event):
    try:
        source = event['sources'][0]['id']
        event_id = event['id']
        category = event['categories'][0]['title'].lower().replace(' ', '-')
        stripped_dt = re.sub('-|:', '', event['geometries'][-1]['date'])
        uid = '{0}_{1}_{2}_{3}_{4}'.format(PRODUCT_PREFIX, category, source, event_id, stripped_dt)
    except:
        raise Exception('failed on {}'.format(event))
    return uid

def is_point_event(event):
    return event['geometries'][-1]['type'] == 'Point'

def build_dataset(event):
    """parse out the relevant dataset parameters and return as dict"""
    time = event['geometries'][-1]['date']

    if is_point_event(event):
        location = build_polygon_geojson(event)
    else:
        location = event['geometries'][-1]
        del location['date']

    label = build_id(event)
    version = VERSION
    return {'label': label, 'starttime': time, 'endtime': time, 'location': location, 'version': version}


def build_metadata(event):
    return event


def convert_epoch_time_to_utc(epoch_timestring):
    dt = datetime.datetime.utcfromtimestamp(epoch_timestring).replace(tzinfo=pytz.UTC)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # use microseconds and convert to milli


def build_point_geojson(event):
    latitude = float(event['geometry']['coordinates'][1])
    longitude = float(event['geometry']['coordinates'][0])
    return {'type': 'point', 'coordinates': [longitude, latitude]}


def shift(latitude, longitude, bearing, distance):
    R = 6378.1  # Radius of the Earth
    bearing = math.pi * bearing / 180  # convert degrees to radians
    lat1 = math.radians(latitude)  # Current lat point converted to radians
    lon1 = math.radians(longitude)  # Current long point converted to radians
    lat2 = math.asin(math.sin(lat1) * math.cos(distance / R) +
                     math.cos(lat1) * math.sin(distance / R) * math.cos(bearing))
    lon2 = lon1 + math.atan2(math.sin(bearing) * math.sin(distance / R) * math.cos(lat1),
                             math.cos(distance / R) - math.sin(lat1) * math.sin(lat2))
    lat2 = math.degrees(lat2)
    lon2 = math.degrees(lon2)
    return [lon2, lat2]


def build_polygon_geojson(event):
    latitude = float(event['geometries'][-1]['coordinates'][1])
    longitude = float(event['geometries'][-1]['coordinates'][0])
    radius = 2.0
    l = range(0, 361, 20)  # Figure out what l, b are, and replace with informative names
    coordinates = []
    for b in l:
        coords = shift(latitude, longitude, b, radius)
        coordinates.append(coords)
    return {'coordinates': [coordinates], 'type': 'polygon'}


def build_product_dir(ds, met):
    label = ds['label']
    dataset_dir = os.path.join(os.getcwd(), label)
    dataset_path = os.path.join(dataset_dir, '{0}.dataset.json'.format(label))
    metadata_path = os.path.join(dataset_dir, '{0}.met.json'.format(label))
    if not os.path.exists(dataset_dir):
        os.mkdir(dataset_dir)
    with open(dataset_path, 'w') as outfile:
        json.dump(ds, outfile)
    with open(metadata_path, 'w') as outfile:
        json.dump(met, outfile)
