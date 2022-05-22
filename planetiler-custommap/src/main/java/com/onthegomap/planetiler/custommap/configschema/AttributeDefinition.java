package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record AttributeDefinition(
  String key,
  @JsonProperty("constant_value") Object constantValue,
  @JsonProperty("tag_value") String tagValue,
  @JsonProperty("include_when") Map<String, Object> includeWhen,
  @JsonProperty("exclude_when") Map<String, Object> excludeWhen,
  @JsonProperty("min_zoom") Byte minZoom,
  @JsonProperty("min_zoom_by_value") Map<Object, Byte> minZoomByValue,
  @JsonProperty("min_tile_cover_size") Double minTileCoverSize
) {}
