#!/usr/bin/env bash

set -eu

mvn verify org.sonarsource.scanner.maven:sonar-maven-plugin:sonar -Pcoverage
