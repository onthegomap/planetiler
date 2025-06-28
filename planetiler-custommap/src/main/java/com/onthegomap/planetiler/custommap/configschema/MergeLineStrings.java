package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;

public record MergeLineStrings(
  @JsonProperty("min_length") Double minLength,
  @JsonProperty("min_length_at_max_zoom") Double minLengthAtMaxZoom,
  @JsonProperty("tolerance") Double tolerance,
  @JsonProperty("tolerance_at_max_zoom") Double toleranceAtMaxZoom,
  @JsonProperty("buffer") Double buffer
) {}
