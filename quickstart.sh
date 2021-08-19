#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAR="flatmap-openmaptiles/target/flatmap-openmaptiles-0.1-SNAPSHOT-fatjar.jar"

echo "Downloading data..."
AREA="${1:-north-america_us_massachusetts}"
./scripts/download-osm.sh "${AREA}"
./scripts/download-other-sources.sh

if [ ! -f "$JAR" ]; then
  echo "Building..."
  mvn -DskipTests=true --projects flatmap-openmaptiles -am clean package
fi

echo "Running..."
java -cp "$JAR" com.onthegomap.flatmap.openmaptiles.OpenMapTilesMain \
  -force=true -input="./data/sources/${AREA}.pbf"
