package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;

public record FeatureItem(
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<String> source,
  @JsonProperty("id") Object id,
  @JsonProperty("min_zoom") Object minZoom,
  @JsonProperty("max_zoom") Object maxZoom,
  @JsonProperty("min_size") Object minSize,
  @JsonProperty("min_size_at_max_zoom") Object minSizeAtMaxZoom,
  @JsonProperty("tolerance") Object tolerance,
  @JsonProperty("tolerance_at_max_zoom") Object toleranceAtMaxZoom,
  @JsonProperty("buffer_pixels") Object bufferPixels,
  @JsonProperty("point_label_grid_pixel_size") PointLabelGridPixelSize pointLabelGridPixelSize,
  @JsonProperty("point_label_grid_limit") PointLabelGridLimit pointLabelGridLimit,
  @JsonProperty("sort_key") Object sortKey,
  @JsonProperty("sort_key_descending") Object sortKeyDescending,
  @JsonProperty FeatureGeometry geometry,
  @JsonProperty("include_when") Object includeWhen,
  @JsonProperty("exclude_when") Object excludeWhen,
  Collection<AttributeDefinition> attributes
) {

  @Override
  public Collection<AttributeDefinition> attributes() {
    return attributes == null ? List.of() : attributes;
  }

  @Override
  public FeatureGeometry geometry() {
    return geometry == null ? FeatureGeometry.ANY : geometry;
  }

  @Override
  public List<String> source() {
    return source == null ? List.of() : source;
  }
}
