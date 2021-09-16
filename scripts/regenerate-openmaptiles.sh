#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAR="flatmap-openmaptiles/target/flatmap-openmaptiles-0.1-SNAPSHOT-fatjar.jar"
TAG="${1:-"v3.12.2"}"
echo "tag=${TAG}"

echo "Building..."
mvn -DskipTests=true --projects flatmap-openmaptiles -am package

echo "Running..."
java -cp "$JAR" com.onthegomap.flatmap.openmaptiles.Generate -tag="${TAG}"
