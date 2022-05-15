package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ZoomFilter(
  TagCriteria tag,
  @JsonProperty("min_zoom") byte minZoom
) {}
