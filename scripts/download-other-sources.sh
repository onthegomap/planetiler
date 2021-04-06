#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

cd "$(git rev-parse --show-cdup)"

mkdir -p data/sources
cd data/sources

wget -N https://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip
wget -N https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip
wget -N https://github.com/lukasmartinelli/osm-lakelines/releases/download/v0.9/lake_centerline.shp.zip
