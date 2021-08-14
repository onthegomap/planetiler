#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

PROJECT="${1:-flatmap-openmaptiles}"

mvn -DskipTests=true --projects "${PROJECT}" -am clean package
