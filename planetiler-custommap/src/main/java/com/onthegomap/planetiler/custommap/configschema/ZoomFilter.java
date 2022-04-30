package com.onthegomap.planetiler.custommap.configschema;

public record ZoomFilter(
  TagCriteria tag,
  byte minZoom
) {}
