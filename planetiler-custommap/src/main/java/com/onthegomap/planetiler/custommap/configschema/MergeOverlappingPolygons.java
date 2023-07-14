package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MergeOverlappingPolygons(
  @JsonProperty("min_area") int minArea
) {}
