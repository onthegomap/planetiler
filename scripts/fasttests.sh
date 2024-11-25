#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

./mvnw -T 1C -Pfast clean test
