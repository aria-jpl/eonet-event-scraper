#!/bin/bash

set -e

#check input args
if [ -z "$1" ] 
then
    echo "No updated_since time specified"
    exit 1
fi
if [ -z "$2" ] 
then
    echo "No geojson product specified"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/query_eonet.py --lookback_days "${1}" --polygon "${2}" --submit
