#!/usr/bin/env bash

set -eu

./mvnw -B -ntp install jib:dockerBuild
