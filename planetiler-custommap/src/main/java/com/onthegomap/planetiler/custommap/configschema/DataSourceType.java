package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonAlias;

public enum DataSourceType {
  @JsonAlias("osm")
  OSM,
  @JsonAlias("shapefile")
  SHAPEFILE
}
