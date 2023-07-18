#!/usr/bin/env bash

# Usage: quickstart.sh {--docker,--jar,--source} {--area=planet,monaco,massachusetts,etc.} [--memory=5g] other args...

set -o errexit
set -o pipefail
set -o nounset

JAVA="${JAVA:-java}"
METHOD="build"
AREA="monaco"
STORAGE="mmap"
PLANETILER_ARGS=()
MEMORY=""
DRY_RUN=""
VERSION="latest"
DOCKER_DIR="$(pwd)/data"
TASK="openmaptiles"
BUILD="true"

# Parse args into env vars
while [[ $# -gt 0 ]]; do
  case $1 in
    --docker) METHOD="docker" ;;
    --dockerdir=*) DOCKER_DIR="${1#*=}" ;;
    --dockerdir) DOCKER_DIR="$2"; shift ;;
    --jar) METHOD="jar" ;;
    --build|--source) METHOD="build" ;;
    --skipbuild) METHOD="build"; BUILD="false" ;;
    --version=*) VERSION="${1#*=}" ;;
    --version) VERSION="$2"; shift ;;

    --area=*) AREA="${1#*=}" ;;
    --area) AREA="$2"; shift ;;
    --planet) AREA="planet" ;;

    --memory=*) MEMORY="${MEMORY:-"-Xmx${1#*=}"}" ;;
    --memory) MEMORY="${MEMORY:-"-Xmx$2"}"; shift ;;
    --ram) STORAGE="ram" ;;

    --dry-run) DRY_RUN="true" ;;

    *)
      # on the first passthrough arg, check if it's instructions to do something besides openmaptiles
      if (( ${#PLANETILER_ARGS[@]} == 0 )); then
        case $1 in
          *openmaptiles*) PLANETILER_ARGS+=("$1") ;;
          -*) PLANETILER_ARGS+=("$1") ;;
          *.yml|*shortbread*|*generate*|*-qa|*example*|*verify*|*custom*|*benchmark*)
            TASK="$1"
            PLANETILER_ARGS+=("$1")
            ;;
          *) AREA="$1" ;;
        esac
      else
        PLANETILER_ARGS+=("$1")
      fi
      ;;
  esac
  shift
done

PLANETILER_ARGS+=("--storage=$STORAGE")
PLANETILER_ARGS+=("--download")
PLANETILER_ARGS+=("--force")

# Configure memory settings based on the area being built
PLANETILER_ARGS+=("--area=$AREA")
case $AREA in
  planet)
    # For extracts, use default nodemap type (sortedtable) and -Xmx (25% of RAM up to 25GB) and hope for the best.
    # You can set --memory=5g if you want to change it.
    PLANETILER_ARGS+=("--nodemap-type=array" "--download-threads=20" "--download-chunk-size-mb=500")
    case "$STORAGE" in
      ram) MEMORY="${MEMORY:-"-Xmx150g"}" ;;
      mmap) MEMORY="${MEMORY:-"-Xmx30g -Xmn15g"}" ;;
    esac
    ;;
  monaco)
    echo "$TASK"
    echo "${PLANETILER_ARGS[*]}"
    if [ "$TASK" == "openmaptiles" ]; then
      if [[ "${PLANETILER_ARGS[*]}" =~ ^.*osm[-_](path|url).*$ ]]; then
        : # don't add monaco args
      else
        # Use mini extracts for monaco
        PLANETILER_ARGS+=("--water-polygons-url=https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/water-polygons-split-3857.zip")
        PLANETILER_ARGS+=("--water-polygons-path=data/sources/monaco-water.zip")
        PLANETILER_ARGS+=("--natural-earth-url=https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/natural_earth_vector.sqlite.zip")
        PLANETILER_ARGS+=("--natural-earth-path=data/sources/monaco-natural_earth_vector.sqlite.zip")
      fi
    fi
    ;;
esac

JVM_ARGS="-XX:+UseParallelGC $MEMORY"

echo "Running planetiler with:"
echo "  METHOD=\"$METHOD\" (change with --docker --jar or --build)"
echo "  JVM_ARGS=\"${JVM_ARGS}\" (change with --memory=Xg)"
echo "  TASK=\"${TASK}\""
echo "  PLANETILER_ARGS=\"${PLANETILER_ARGS[*]}\""
echo "  DRY_RUN=\"${DRY_RUN:-false}\""
echo ""

if [ "$DRY_RUN" == "true" ]; then
  echo "Without --dry-run, will run commands:"
fi

function run() {
  command="${*//&/\&}"
  echo "$ $command"
  if [ "$DRY_RUN" != "true" ]; then
    eval "$command"
  fi
}

function check_java_version() {
  if [ "$DRY_RUN" != "true" ]; then
    if [ -z "$(which java)" ]; then
      echo "java not found on path"
      exit 1
    else
      OUTPUT="$($JAVA -jar "$1" --help 2>&1 || echo OK)"
      if [[ "$OUTPUT" =~ "UnsupportedClassVersionError" ]]; then
        echo "Wrong version of java installed, need at least 17 but found:"
        $JAVA --version
        exit 1
      fi
    fi
  fi
}

# Run planetiler using docker, jar file, or build from source
case $METHOD in
  docker)
    run docker run -e JAVA_TOOL_OPTIONS=\'"${JVM_ARGS}"\' -v "$DOCKER_DIR":/data "ghcr.io/onthegomap/planetiler:${VERSION}" "${PLANETILER_ARGS[@]}"
    ;;
  jar)
    echo "Downloading latest planetiler release..."
    run wget -nc "https://github.com/onthegomap/planetiler/releases/${VERSION}/download/planetiler.jar"
    check_java_version planetiler.jar
    run "$JAVA" "${JVM_ARGS}" -jar planetiler.jar "${PLANETILER_ARGS[@]}"
    ;;
  build)
    if [ "$BUILD" == "true" ]; then
      echo "Building planetiler..."
      run ./mvnw -q -DskipTests --projects planetiler-dist -am clean package
    fi
    run "$JAVA" "${JVM_ARGS}" -jar planetiler-dist/target/*with-deps.jar "${PLANETILER_ARGS[@]}"
    ;;
esac
