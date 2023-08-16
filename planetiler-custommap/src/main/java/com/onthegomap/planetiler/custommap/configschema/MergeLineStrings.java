package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MergeLineStrings(
  @JsonProperty("min_length") double minLength,
  @JsonProperty("tolerance") double tolerance,
  @JsonProperty("buffer") double buffer
) {}
