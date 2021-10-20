#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

find . -name '*.md' -exec markdown-link-check --progress --config .github/workflows/docs_mlc_config.json {} \;
