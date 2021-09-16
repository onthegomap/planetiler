#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

JAR="flatmap-openmaptiles/target/flatmap-openmaptiles-0.1-SNAPSHOT-fatjar.jar"

AREA="${1:-monaco}"
shift

echo "Building..."
mvn -DskipTests=true --projects flatmap-openmaptiles -am package

echo "Running..."
java -jar "$JAR" --force=true --area="${AREA}" $*
