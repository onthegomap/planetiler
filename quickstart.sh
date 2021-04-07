#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAR="target/flatmap-0.1-SNAPSHOT-jar-with-dependencies.jar"

echo "Downloading data..."
AREA="${1:-north-america_us_massachusetts}"
./scripts/download-osm.sh "${AREA}"
./scripts/download-other-sources.sh

if [ ! -f "$JAR" ]; then
  echo "Building..."
  mvn -DskipTests=true package
fi

echo "Running..."
java -Dinput="./data/sources/${AREA}.pbf" \
  -cp "$JAR" \
  com.onthegomap.flatmap.profiles.OpenMapTilesProfile
