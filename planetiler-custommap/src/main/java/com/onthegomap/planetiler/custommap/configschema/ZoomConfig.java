package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public record ZoomConfig(
  Byte minZoom,
  Byte maxZoom,
  Collection<ZoomFilter> zoomFilter
) {}
