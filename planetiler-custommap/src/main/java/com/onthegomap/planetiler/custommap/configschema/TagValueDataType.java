package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum TagValueDataType {
  @JsonProperty("string")
  STRING,
  @JsonProperty("long")
  LONG,
  @JsonProperty("direction")
  DIRECTION,
  @JsonProperty("boolean")
  BOOLEAN
}
