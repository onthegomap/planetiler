package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;

public record ZoomConfig(
  @JsonProperty("min_zoom") Byte minZoom,
  @JsonProperty("max_zoom") Byte maxZoom,
  @JsonProperty("zoom_filter") Collection<ZoomFilter> zoomFilter
) {}
