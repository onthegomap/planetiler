#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

AREA="${1:-north-america_us_massachusetts}"
./scripts/download-osm.sh "${AREA}"
./scripts/download-other-sources.sh
