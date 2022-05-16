package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.geo.GeometryType;
import java.util.Collection;

public record FeatureItem(
  Collection<String> sources,
  @JsonProperty("min_zoom") Byte minZoom,
  @JsonProperty("max_zoom") Byte maxZoom,
  GeometryType geometry,
  @JsonProperty("include_when") TagCriteria includeWhen,
  @JsonProperty("exclude_when") TagCriteria excludeWhen,
  Collection<AttributeDefinition> attributes
) {}
