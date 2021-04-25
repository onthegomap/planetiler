#!/usr/bin/env bash
set -ex

protoc --java_out=src/main/java/ src/main/resources/vector_tile.proto
