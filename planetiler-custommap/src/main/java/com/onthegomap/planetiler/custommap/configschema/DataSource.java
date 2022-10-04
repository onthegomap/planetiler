package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DataSource(
  DataSourceType type,
  Object url,
  @JsonProperty("local_path") Object localPath
) {}
