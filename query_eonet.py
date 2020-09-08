#!/usr/bin/env python
'''
Queries the EONET feed for events over a given input range.
Publishes event products that pass the filter.
'''

from __future__ import print_function
import json
import os
import datetime
import argparse
import copy
import dateutil.parser as dt_parser
import pytz
import requests
from shapely.geometry import shape, Polygon, Point
import redis
from hysds_commons.net_utils import get_container_host_ip
import build_event_product

POOL = None
REDIS_KEY = 'eonet_last_query'


def main(starttime=None, endtime=None, lookback_days=None, status=None, source=None, polygon=None, test=False):
    """Runs tests, queries eonet, filters events, then builds products"""

    # run test aoi first if test is provided
    if test:
        build_event_product.build_hysds_product(get_test_event())
        return

    now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3]

    query_string = build_query(lookback_days, status, source, polygon, now)
    print('Running EONET query: {0}'.format(query_string))

    response = run_query(query_string)
    print('query returned {} results'.format(len(response['events'])))

    events = filter_response(response, starttime, endtime, polygon)
    print('filtered results returned {0} total'.format(len(events)))

    for event in events:
        try:
            # submit an event for each product date/location
            geometries = copy.deepcopy(event['geometries'])
            for geometry in geometries:
                event['geometries'] = [geometry]
                build_event_product.build_hysds_product(event)
        except Exception as err:
            print('failed on build {} with err: {}'.format(event, err))

    if redis:
        # TODO: sort out redis authentication required error after prototype is complete  
        pass
        # redis_set(REDIS_KEY, now) #sets the redis query to the runtime


def build_query(lookback_days, status, source, polygon_string, now):
    '''builds a query url from the input filter params. returns the url'''

    query = 'https://eonet.sci.gsfc.nasa.gov/api/v2.1/events?limit=10000'

    # build query params
    if lookback_days == 'redis':
        # use dt of last query from redis, & find the number of days lookback that requires
        redis_str = get_redis_time()
        if not redis_str is None:
            redis_dt = dt_parser.parse(redis_str).replace(tzinfo=pytz.UTC)
            num_days = (dt_parser.parse(now).replace(tzinfo=pytz.UTC) - redis_dt).days + 1
            query += '&days={}'.format(num_days)
    elif lookback_days:
        query += '&days={0}'.format(lookback_days)
    if status:
        query += '&status={0}'.format(status)
    if source:
        query += '&source={0}'.format(source)

    return query


def run_query(query_url):

    try:
        session = requests.session()
        response = session.get(query_url, timeout=45)
    except Exception as e:
        raise Exception('Query failed: {0}\nquery: {1}'.format(e, query_url))

    if response.status_code != 200:
        raise Exception("{0} status for query: {1}".format(response.status_code, query_url))

    return json.loads(response.text)


def filter_response(response, starttime, endtime, polygon_string):
    '''validate response and filter through polygon client-side'''

    # remove events without a date
    dated_events = []
    for event in response['events']:
        temp_event = copy.deepcopy(event)
        temp_event['geometries'] = [x for x in copy.deepcopy(event['geometries']) if 'date' in x.keys()]
        dated_events.append(temp_event)
        print(temp_event['geometries'][0]['date'])

    prefiltered_events = [event for event in dated_events if len(event['geometries']) > 0]

    # run the spatial and temporal filters
    for event in prefiltered_events:
        if polygon_string:
            event['geometries'] = [geometry for geometry in event['geometries'] if validate_spatial_coverage(geometry, polygon_string)]
        if starttime and endtime:
            event['geometries'] = [geometry for geometry in event['geometries'] if validate_temporal_coverage(geometry, starttime, endtime)]

    return [event for event in prefiltered_events if len(event['geometries']) > 0]


def validate_temporal_coverage(location, start_time, end_time):
    '''validate that the date in location is betweens start and endtime strings'''

    start_dt = dt_parser.parse(start_time).replace(tzinfo=pytz.UTC)
    end_dt = dt_parser.parse(end_time).replace(tzinfo=pytz.UTC)
    event_dt = dt_parser.parse(location['date']).replace(tzinfo=pytz.UTC)

    if event_dt > start_dt and event_dt < end_dt:
        return True

    return False


def validate_geojson(polygon_string):

    if polygon_string is None:
        return False

    try:
        json_polygon = json.loads(polygon_string)
        polygon = Polygon(json_polygon)
    except:
        return False
    return True


def get_polygon(polygon_string):
    json_polygon = json.loads(polygon_string)
    return Polygon(json_polygon)


def validate_spatial_coverage(location, polygon_string):
    json_polygon = json.loads(polygon_string)
    polygon = Polygon(json_polygon)
    event_shape = shape(location)

    return is_covered(event_shape, polygon)


def is_covered(event_shape, polygon):
    return polygon.intersects(event_shape)


def validate_user_time(input_time):
    '''parses the time and returns in UTC format'''
    try:
        user_time = dt_parser.parse(input_time).replace(tzinfo=pytz.UTC)
    except:
        raise Exception('Unable to parse input time: {0}'.format(input_time))
    return user_time.strftime('%Y-%m-%dT%H:%M:%S')


def validate_decimal(input_decimal):
    '''returns a string to 0.1 sig fig'''
    try:
        return "{:.1f}".format(float(input_decimal))
    except:
        print('Input value invalid: {0}'.format(input_decimal))


def get_redis_time():
    '''get the last successful runtime from redis'''
    return redis_get(REDIS_KEY)


def redis_get(key):
    '''returns the value of the given redis key'''
    global POOL
    redis_url = 'redis://%s' % get_container_host_ip()
    POOL = redis.ConnectionPool.from_url(redis_url)
    rds = redis.StrictRedis(connection_pool=POOL)
    value = rds.get(key)
    return value


def redis_set(key, value):
    '''set redis key to the given value'''
    global POOL
    redis_url = 'redis://%s' % get_container_host_ip()
    POOL = redis.ConnectionPool.from_url(redis_url)
    rds = redis.StrictRedis(connection_pool=POOL)
    rds.set(key, value)
    return value


def get_test_event():
    '''loads test_event.json file and returns the dict'''
    test_json = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_event.json')
    return json.load(open(test_json))


def argument_parser():
    '''
    Construct a parser to parse arguments
    @return argparse parser
    '''
    parse = argparse.ArgumentParser(description="Run EONET query with given parameters")
    parse.add_argument("--starttime", required=False, default=None, help="Start time for query range.",
                       dest="starttime")
    parse.add_argument("--endtime", required=False, default=None, help="End time for query range.", dest="endtime")
    parse.add_argument("--lookback_days", required=False, default=None,
                       help="Number of days to lookback in query. Use 'redis': will use redis to query for products updated since last successful query time.",
                       dest="lookback_days")
    parse.add_argument("--status", required=False, default=None, choices=['open', 'closed'],
                       help="Status of event. open or closed", dest="status")
    parse.add_argument("--source", required=False, default=None,
                       help="Query over single source, sources at: https://eonet.sci.gsfc.nasa.gov/api/v2.1/sources",
                       dest="source")
    parse.add_argument("--slack_notification", required=False, default=False,
                       help="Key for slack notification, will notify via slack if provided.", dest="slack_notification")
    parse.add_argument("--polygon", required=False, default=None, help="Geojson polygon filter", dest="polygon")
    parse.add_argument("--test", required=False, default=False, action="store_true",
                       help="Run a test submission. Overrides all other params", dest="test")
    return parse


if __name__ == '__main__':
    args = argument_parser().parse_args()
    main(starttime=args.starttime, endtime=args.endtime, lookback_days=args.lookback_days, status=args.status,
         source=args.source, polygon=args.polygon, test=args.test)
