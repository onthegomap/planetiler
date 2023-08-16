package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MergePolygons(
  @JsonProperty("min_area") double minArea
) {}
