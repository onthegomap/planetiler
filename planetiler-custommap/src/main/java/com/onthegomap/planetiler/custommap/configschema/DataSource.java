package com.onthegomap.planetiler.custommap.configschema;

public record DataSource(
  DataSourceType type,
  String url
) {}
