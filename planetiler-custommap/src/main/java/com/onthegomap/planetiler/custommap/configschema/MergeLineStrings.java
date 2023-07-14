package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MergeLineStrings(
  @JsonProperty("min_length") double minLength,
  @JsonProperty() double tolerance,
  @JsonProperty() double buffer
) {}
