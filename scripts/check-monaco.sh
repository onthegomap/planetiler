#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

java -ea -jar flatmap-dist/target/*-with-deps.jar verify-monaco $*
