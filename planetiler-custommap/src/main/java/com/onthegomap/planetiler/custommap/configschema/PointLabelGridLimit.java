package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PointLabelGridLimit(
  @JsonProperty("maxzoom") int maxZoom,
  @JsonProperty("limit") int limit
) {}
