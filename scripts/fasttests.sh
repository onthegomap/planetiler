#!/usr/bin/env bash
# Runs the tests about 4x faster than `./mvnw clean verify` by running test classes and methods concurrently.
# Usage: scripts/fasttests.sh [--skip-slow] [extra mvn args...]

set -o errexit
set -o pipefail
set -o nounset

if [[ "${1:-}" == "--skip-slow" ]]; then
  shift
  set -- '-Dgroups=!slow' "$@"
fi

./mvnw -q -T 1C clean test "$@" -Djunit.jupiter.execution.parallel.enabled=true -Djunit.jupiter.execution.parallel.mode.default=concurrent -Djunit.jupiter.execution.parallel.mode.classes.default=concurrent
