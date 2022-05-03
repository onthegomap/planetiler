package com.onthegomap.planetiler.custommap.configschema;

public record AttributeDefinition(
  String key,
  Object constantValue,
  String tagValue,
  TagCriteria includeWhen,
  TagCriteria excludeWhen,
  int minZoom,
  Double minTileCoverSize
) {}
