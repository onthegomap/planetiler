#!/usr/bin/env bash

set -eu

./mvnw verify org.sonarsource.scanner.maven:sonar-maven-plugin:sonar -Pcoverage
