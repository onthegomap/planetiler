#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -x

if test "$#" -ne 1; then
  echo "Usage: ubuntu_vm_setup.sh user@ip"
  exit 1
fi

"$(dirname "$0")"/build.sh

rsync -avzP openmaptiles/target/flatmap-openmaptiles-0.1-SNAPSHOT-fatjar.jar "${1}":flatmap.jar
scp scripts/download-other-sources.sh "${1}":download-other-sources.sh
scp scripts/download-osm.sh "${1}":download-osm.sh
ssh -t "${1}" "bash -s" <<EOF
wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add - && \
add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
apt-get update -y && \
apt-get install adoptopenjdk-16-hotspot-jre -y && \
./download-other-sources.sh
EOF
