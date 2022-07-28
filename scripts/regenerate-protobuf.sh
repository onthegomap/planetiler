#!/usr/bin/env bash
set -ex

echo "Regenerating..."
protoc --java_out=annotate_code:planetiler-core/src/main/java/ planetiler-core/src/main/resources/vector_tile_proto.proto
protoc --java_out=annotate_code:planetiler-core/src/main/java/ planetiler-core/src/main/resources/fileformat.proto
protoc --java_out=annotate_code:planetiler-core/src/main/java/ planetiler-core/src/main/resources/osmformat.proto

echo "Formatting..."
./scripts/format.sh
