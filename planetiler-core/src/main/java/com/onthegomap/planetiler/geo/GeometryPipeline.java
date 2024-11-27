package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.Geometry;

@FunctionalInterface
public interface GeometryPipeline {
  Geometry apply(Geometry input);

  default GeometryPipeline andThen(GeometryPipeline other) {
    return input -> other.apply(apply(input));
  }
}
