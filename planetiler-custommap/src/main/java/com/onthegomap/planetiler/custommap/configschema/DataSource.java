package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;

public record DataSource(
  DataSourceType type,
  String url,
  @JsonProperty("local_path") Path localPath
) {}
