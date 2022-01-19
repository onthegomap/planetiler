#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

TAG="${1:-"v3.13"}"
echo "tag=${TAG}"

echo "Building..."
./mvnw -DskipTests=true --projects planetiler-dist -am package

echo "Running..."
java -cp planetiler-dist/target/*-with-deps.jar com.onthegomap.planetiler.basemap.Generate -tag="${TAG}"
