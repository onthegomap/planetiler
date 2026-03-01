package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PointLabelGridPixelSize(
  @JsonProperty("maxzoom") int maxZoom,
  @JsonProperty("size") double size
) {}
