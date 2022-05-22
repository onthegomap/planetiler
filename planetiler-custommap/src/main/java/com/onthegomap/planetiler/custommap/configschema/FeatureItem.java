package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.geo.GeometryType;
import java.util.Collection;
import java.util.Map;

public record FeatureItem(
  Collection<String> sources,
  @JsonProperty("min_zoom") Integer minZoom,
  @JsonProperty("max_zoom") Integer maxZoom,
  GeometryType geometry,
  @JsonProperty("zoom_override") Collection<ZoomOverride> zoom,
  @JsonProperty("include_when") Map<String, Object> includeWhen,
  @JsonProperty("exclude_when") Map<String, Object> excludeWhen,
  Collection<AttributeDefinition> attributes
) {}
