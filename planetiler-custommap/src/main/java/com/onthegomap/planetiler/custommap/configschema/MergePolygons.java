package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MergePolygons(
  @JsonProperty("min_area") Double minArea,
  @JsonProperty("min_area_at_max_zoom") Double minAreaAtMaxZoom,
  @JsonProperty("tolerance") Double tolerance,
  @JsonProperty("tolerance_at_max_zoom") Double toleranceAtMaxZoom
) {}
