#!/usr/bin/env bash
set -ex

protoc --java_out=core/src/main/java/ core/src/main/resources/vector_tile.proto
