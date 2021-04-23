package com.onthegomap.flatmap;

import java.util.Map;
import org.locationtech.jts.geom.Geometry;

public interface SourceFeature {

  Geometry geometry();

  default void setTag(String key, Object value) {
    properties().put(key, value);
  }

  Map<String, Object> properties();

  default Object getTag(String name) {
    return properties().get(name);
  }

  // lazy geometry
  // lazy centroid
  // lazy area
}
