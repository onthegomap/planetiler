#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

mvn -DskipTests=true --projects openmaptiles -am package
