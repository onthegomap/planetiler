package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.Geometry;

@FunctionalInterface
public interface GeometryPipeline {
  Geometry apply(Geometry input);

  default GeometryPipeline andThen(GeometryPipeline other) {
    return input -> other.apply(apply(input));
  }

  /**
   * Returns a pipeline that simplifies input geometries using the {@link VWSimplifier Visvalingam Whyatt algorithm}.
   */
  static VWSimplifier simplifyVW(double tolerance) {
    return new VWSimplifier().setTolerance(tolerance);
  }

  /**
   * Returns a pipeline that simplifies input geometries using the {@link DouglasPeuckerSimplifier Douglas Peucker
   * algorithm}.
   */
  static DouglasPeuckerSimplifier simplifyDP(double tolerance) {
    return new DouglasPeuckerSimplifier(tolerance);
  }
}
