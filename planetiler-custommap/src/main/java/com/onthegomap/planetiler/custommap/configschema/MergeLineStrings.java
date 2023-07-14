package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MergeLineStrings(
  @JsonProperty("min_length") int minLength,
  @JsonProperty() int tolerance,
  @JsonProperty() int buffer
) {}
