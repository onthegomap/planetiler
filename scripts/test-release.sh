#!/usr/bin/env bash

set -exuo pipefail

version="${1:-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)}"

if [ "${SKIP_EXAMPLE_PROJECT:-false}" == "true" ]; then
  echo "skipping example project"
else
  echo "::group::Test building example project"
  (cd planetiler-examples && mvn -B -ntp -Dplanetiler.version="${version}" verify --file standalone.pom.xml)
  echo "::endgroup::"
fi

echo "Test java build"
echo "::group::OpenMapTiles monaco (java)"
rm -f data/out*.mbtiles
java -jar planetiler-dist/target/*with-deps.jar --download --area=monaco --output=data/jar-monaco.mbtiles
./scripts/check-monaco.sh data/jar-monaco.mbtiles
echo "::endgroup::"
echo "::group::Example (java)"
java -jar planetiler-dist/target/*with-deps.jar example-toilets --download --area=monaco --output=data/jar-example.mbtiles
./scripts/check-mbtiles.sh data/jar-example.mbtiles
echo "::endgroup::"

echo "::endgroup::"
echo "::group::OpenMapTiles monaco (docker)"
docker run -v "$(pwd)/data":/data ghcr.io/onthegomap/planetiler:"${version}" --area=monaco --output=data/docker-monaco.mbtiles
./scripts/check-monaco.sh data/docker-monaco.mbtiles
echo "::endgroup::"
echo "::group::Example (docker)"
docker run -v "$(pwd)/data":/data ghcr.io/onthegomap/planetiler:"${version}" example-toilets --area=monaco --output=data/docker-example.mbtiles
./scripts/check-mbtiles.sh data/docker-example.mbtiles
echo "::endgroup::"

echo "::group::Compare"
java -jar planetiler-dist/target/*with-deps.jar compare data/jar-monaco.mbtiles data/docker-monaco.mbtiles
echo "::endgroup::"
