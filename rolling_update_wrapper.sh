#!/bin/bash

set -e

# Define constants
max_eonet_curation_delay="7"
whole_world_polygon="[[-180,-90],[-180,90],[180,90],[180,-90],[-180,-90]]"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${DIR}/submit_eonet_query.py --lookback_days ${max_eonet_curation_delay} --polygon ${whole_world_polygon}
