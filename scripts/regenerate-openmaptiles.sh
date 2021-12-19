#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

TAG="${1:-"v3.12.2"}"
echo "tag=${TAG}"

echo "Building..."
./mvnw -DskipTests=true --projects flatmap-dist -am package

echo "Running..."
java -cp flatmap-dist/target/*-with-deps.jar com.onthegomap.flatmap.basemap.Generate -tag="${TAG}"
