#!/usr/bin/env bash

./scripts/build.sh
#docker build -t gdal .
docker run \
  -v `pwd`/planetiler-dist/target/planetiler-dist-0.6-SNAPSHOT-with-deps.jar:/run.jar \
  -v `pwd`/data:/data gdal -cp /run.jar com.onthegomap.planetiler.util.Gdal


sudo docker run -v /data:/data gdal -jar /data/planetiler.jar asterv3
