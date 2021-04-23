package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.SourceFeature;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

public record ReaderFeature(Geometry geometry, Map<String, Object> properties) implements SourceFeature {

  public ReaderFeature(Geometry geometry, int numProperties) {
    this(geometry, new HashMap<>(numProperties));
  }

}
