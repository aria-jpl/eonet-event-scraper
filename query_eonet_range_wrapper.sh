#!/bin/bash

set -e

#check input args
if [ -z "$1" ] 
then
    echo "No starttime specified"
    exit 1
fi
if [ -z "$2" ] 
then
    echo "No endtime specified"
    exit 1
fi
if [ -z "$3" ]
then
    echo "No geojson product specified"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/query_eonet.py --starttime "${1}" --endtime "${2}" --polygon "${3}" --submit
