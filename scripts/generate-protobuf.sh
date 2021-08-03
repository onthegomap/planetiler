#!/usr/bin/env bash
set -ex

protoc --java_out=flatmap-core/src/main/java/ flatmap-core/src/main/resources/vector_tile.proto
