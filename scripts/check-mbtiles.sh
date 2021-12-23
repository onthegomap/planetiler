#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

java -ea -jar planetiler-dist/target/*-with-deps.jar verify-mbtiles $*
