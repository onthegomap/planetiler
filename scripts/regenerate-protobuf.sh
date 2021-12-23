#!/usr/bin/env bash
set -ex

protoc --java_out=planetiler-core/src/main/java/ planetiler-core/src/main/resources/vector_tile_proto.proto
