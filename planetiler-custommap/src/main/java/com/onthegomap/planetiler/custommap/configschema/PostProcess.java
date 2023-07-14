package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PostProcess(
  @JsonProperty("merge_line_strings") MergeLineStrings mergeLineStrings,
  @JsonProperty("merge_overlapping_polygons") MergeOverlappingPolygons mergeOverlappingPolygons
) {}
