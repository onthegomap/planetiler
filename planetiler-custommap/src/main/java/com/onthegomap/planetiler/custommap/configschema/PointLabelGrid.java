package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PointLabelGrid(
  @JsonProperty("pixel_size") PointLabelGridPixelSize pixelSize,
  @JsonProperty("limit") PointLabelGridLimit limit
) {}
