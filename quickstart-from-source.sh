#!/usr/bin/env bash

set -e

AREA="${1:-monaco}"
shift || echo ""

echo "Will build planetiler, download sources for ${AREA}, and make a map."
echo "This requires at least 1GB of disk space. Press Ctrl+C to exit..."
sleep 5

echo "Building..."
./mvnw -DskipTests=true --projects planetiler-dist -am package

echo "Running..."
java -jar planetiler-dist/target/*with-deps.jar --force --download --area="${AREA}" $*
