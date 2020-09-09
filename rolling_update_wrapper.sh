#!/bin/bash

# This is placed in the directory referred to in the following example crontab entry
# 00 * * * * source /export/home/hysdsops/verdi/bin/activate && /export/home/hysdsops/aria_cron_jobs/eonet/rolling_update_wrapper.sh 2>&1

branch=${1:-"main"}

set -e

# Define constants
queue="edunn-jplnet-dev"
max_eonet_curation_delay="15"
whole_world_polygon="[[-180,-90],[-180,90],[180,90],[180,-90],[-180,-90]]"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LD_LIBRARY_PATH=$HOME/conda/lib:/usr/lib:/usr/lib64:/usr/local/lib:$LD_LIBRARY_PATH ${DIR}/submit_eonet_query.py --lookback_days ${max_eonet_curation_delay} --polygon ${whole_world_polygon} --version $branch --queue $queue