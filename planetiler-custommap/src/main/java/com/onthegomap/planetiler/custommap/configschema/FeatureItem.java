package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public record FeatureItem(
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<String> source,
  @JsonProperty("min_zoom") Integer minZoom,
  @JsonProperty("max_zoom") Integer maxZoom,
  @JsonProperty(required = true) FeatureGeometry geometry,
  @JsonProperty("zoom_override") Collection<ZoomOverride> zoom,
  @JsonProperty("include_when") Map<String, Object> includeWhen,
  @JsonProperty("exclude_when") Map<String, Object> excludeWhen,
  Collection<AttributeDefinition> attributes
) {

  @Override
  public Collection<AttributeDefinition> attributes() {
    return attributes == null ? List.of() : attributes;
  }
}
