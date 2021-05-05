#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -x

docker run --rm -it -v "$(git rev-parse --show-toplevel)/data":/data -p 8080:8080 \
  maptiler/tileserver-gl -p 8080
