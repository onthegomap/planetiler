package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum DataSourceType {
  @JsonProperty("osm")
  OSM,
  @JsonProperty("shapefile")
  SHAPEFILE
}
