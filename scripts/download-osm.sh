#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

if test "$#" -ne 1; then
  echo "Usage: download-osm.sh <area>"
  echo "Example: download-osm.sh north-america_us_massachusetts"
  exit 1
fi

cd "$(git rev-parse --show-cdup)"
mkdir -p data/sources

LINK=$(echo "${1}" | tr '_' '/')
LINK="http://download.geofabrik.de/$LINK-latest.osm.pbf"

(
  cd data/sources
  wget -N "${LINK}"
)
