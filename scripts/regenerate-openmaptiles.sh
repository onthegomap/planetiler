#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

TAG="${1:-"v3.13.1"}"
echo "tag=${TAG}"

BASE_URL="${2:-"https://raw.githubusercontent.com/openmaptiles/openmaptiles/"}"
echo "base-url=${BASE_URL}"

echo "Building..."
./mvnw -DskipTests=true --projects planetiler-dist -am package

echo "Running..."
java -cp planetiler-dist/target/*-with-deps.jar com.onthegomap.planetiler.openmaptiles.Generate -tag="${TAG}" -base-url="${BASE_URL}"

echo "Formatting..."
./scripts/format.sh
