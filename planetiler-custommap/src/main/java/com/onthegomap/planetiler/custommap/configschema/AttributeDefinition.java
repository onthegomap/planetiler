package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AttributeDefinition(
  String key,
  @JsonProperty("constant_value") Object constantValue,
  @JsonProperty("tag_value") String tagValue,
  @JsonProperty("include_when") TagCriteria includeWhen,
  @JsonProperty("exclude_when") TagCriteria excludeWhen,
  @JsonProperty("min_zoom") int minZoom,
  @JsonProperty("min_tile_cover_size") Double minTileCoverSize
) {}
