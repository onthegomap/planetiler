#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAR="flatmap-dist/target/*-with-deps.jar"
TAG="${1:-"v3.12.2"}"
echo "tag=${TAG}"

echo "Building..."
./mvnw -DskipTests=true --projects flatmap-basemap -am package

echo "Running..."
java -cp "$JAR" com.onthegomap.flatmap.basemap.Generate -tag="${TAG}"
