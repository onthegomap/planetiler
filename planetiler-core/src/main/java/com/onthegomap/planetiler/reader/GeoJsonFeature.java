package com.onthegomap.planetiler.reader;

import java.util.Map;
import org.locationtech.jts.geom.Geometry;

public record GeoJsonFeature(Geometry geometry, @Override Map<String, Object> tags)
  implements WithTags {}
