#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)}"

TAGS=""
if [ -n "${IMAGE_TAGS:-}" ]; then
  TAGS="-Djib.to.tags=${IMAGE_TAGS// /}"
fi

./mvnw -B -ntp -DskipTests "${TAGS}" -Pjib-multi-arch \
  -Dimage.version="${VERSION}" \
  -Djib.to.auth.username="${GITHUB_ACTOR}" \
  -Djib.to.auth.password="${GITHUB_TOKEN}" \
  package jib:build --file pom.xml

./mvnw -B -Dgpg.passphrase="${OSSRH_GPG_SECRET_KEY_PASSWORD}" -DskipTests -Prelease -Pflatten deploy
