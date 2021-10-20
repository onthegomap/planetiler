#!/usr/bin/env bash

set -e

AREA="${1:-monaco}"
shift || echo "using area=monaco"

echo "Building..."
./mvnw -DskipTests=true --projects flatmap-dist -am package

echo "Running..."
java -jar flatmap-dist/target/*with-deps.jar --force=true --area="${AREA}" $*
