#!/usr/bin/env python

'''
Submits a standard PAGER job via a REST call, without HySDS reqs
'''

from __future__ import print_function
import os
import re
import json
import argparse
import requests
from datetime import datetime
import celeryconfig

def main(lookback, polygon, version, queue, priority, tags):
    '''
    submits a job to mozart to start pager job
    '''
    # submit mozart job
    job_submit_url = os.path.join(celeryconfig.MOZART_URL, 'api/v0.2/job/submit')
    job_params = '{"lookback_days": "%s", "geojson_polygon": "%s"}' % (lookback, polygon)
    params = {
        'queue': queue,
        'priority': priority,
        'tags': '[{0}]'.format(parse_job_tags(tags)),
        'type': 'job-query_eonet:%s' % version,
        'params': job_params,
        'enable_dedup': False
    }
    print('submitting jobs with params: %s' %  job_params)
    r = requests.post(job_submit_url, params=params, verify=False)
    if r.status_code != 200:
        r.raise_for_status()
    result = r.json()
    now = datetime.now()
    if 'result' in result.keys() and 'success' in result.keys():
        if result['success'] == True:
            job_id = result['result']
            print('%s: submitted USGSS NEIC job version: %s job_id: %s' % (now, version, job_id))
        else:
            print('%s: job not submitted successfully: %s' % (now, result))
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)

def parse_job_tags(tag_string):
    if tag_string == None or tag_string == '':
        return ''
    tag_list = tag_string.split(',')
    tag_list = ['"{0}"'.format(tag) for tag in tag_list]
    return ','.join(tag_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lookback_days', help='Number of days to lookback for events. Use redis for last query time', dest='lookback', required=False, default='1')
    parser.add_argument('--polygon', help='geojson polygon filter', dest='polygon', required=False, default="[[-180,-90],[-180,90],[180,90],[180,-90],[-180,-90]]")
    parser.add_argument('--version', help='release version, eg "master" or "release-20180615"', dest='version', required=False, default='master')
    parser.add_argument('--queue', help='Job queue', dest='queue', required=False, default='factotum-job_worker-small')
    parser.add_argument('--priority', help='Job priority', dest='priority', required=False, default='5')
    parser.add_argument('--tags', help='Job tags. Use a comma separated list for more than one', dest='tags', required=False, default='usgs_neic_query')
    args = parser.parse_args()

    main(args.lookback, args.polygon, args.version, args.queue, args.priority, args.tags)
