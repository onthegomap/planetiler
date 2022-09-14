package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record AttributeDefinition(
  String key,
  @JsonProperty("value") Object value,
  @JsonProperty("tag_value") String tagValue,
  @JsonProperty("include_when") Object includeWhen,
  @JsonProperty("exclude_when") Object excludeWhen,
  @JsonProperty("min_zoom") Object minZoom,
  @JsonProperty("min_zoom_by_value") Map<Object, Integer> minZoomByValue,
  @JsonProperty("min_tile_cover_size") Double minTileCoverSize,
  @JsonProperty("type") String type,
  @JsonProperty("else") Object fallback
) {}
