#!/usr/bin/env bash

set -e

AREA="${1:-monaco}"
shift || echo "using area=monaco"

echo "Building..."
./mvnw -DskipTests=true --projects planetiler-dist -am package

echo "Running..."
java -jar planetiler-dist/target/*with-deps.jar --force=true --area="${AREA}" $*
