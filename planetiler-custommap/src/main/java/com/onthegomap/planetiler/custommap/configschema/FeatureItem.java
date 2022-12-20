package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;

public record FeatureItem(
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<String> source,
  @JsonProperty("min_zoom") Object minZoom,
  @JsonProperty("max_zoom") Object maxZoom,
  @JsonProperty("min_pixel_size") Object minPixelSize,
  @JsonProperty(required = true) FeatureGeometry geometry,
  @JsonProperty("include_when") Object includeWhen,
  @JsonProperty("exclude_when") Object excludeWhen,
  Collection<AttributeDefinition> attributes
) {

  @Override
  public Collection<AttributeDefinition> attributes() {
    return attributes == null ? List.of() : attributes;
  }
}
