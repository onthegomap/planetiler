#!/usr/bin/env bash
set -euo pipefail

version="${1:-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)}"

echo -n "${OSSRH_GPG_SECRET_KEY}" | base64 --decode | gpg --batch --import

docker image push ghcr.io/onthegomap/flatmap:"${version}"

TAGS="${IMAGE_TAGS:-}"
for TAG in ${TAGS//,/ }
do
  echo "Pushing tag ${TAG}"
  docker image tag ghcr.io/onthegomap/flatmap:"${version}" ghcr.io/onthegomap/flatmap:"${TAG}"
  docker image push ghcr.io/onthegomap/flatmap:"${TAG}"
done

./mvnw -B -Dgpg.passphrase="${OSSRH_GPG_SECRET_KEY_PASSWORD}" -DskipTests -Prelease deploy
