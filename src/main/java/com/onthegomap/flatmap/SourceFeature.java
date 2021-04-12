package com.onthegomap.flatmap;

import org.locationtech.jts.geom.Geometry;

public interface SourceFeature {

  Geometry getGeometry();
  // props
  // lazy geometry
  // lazy centroid
  // lazy area
}
