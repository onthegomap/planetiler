#!/usr/bin/env bash

set -eu

mvn sonar:sonar \
   -Dsonar.projectKey=planetiler \
   -Dsonar.organization=onthegomap \
   -Dsonar.host.url=https://sonarcloud.io \
   -Dsonar.login="${SONAR_TOKEN}"
