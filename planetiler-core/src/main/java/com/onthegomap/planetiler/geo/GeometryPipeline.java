package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.util.ZoomFunction;
import org.locationtech.jts.geom.Geometry;

@FunctionalInterface
public interface GeometryPipeline extends ZoomFunction<GeometryPipeline> {
  GeometryPipeline NOOP = g -> g;

  Geometry apply(Geometry input);

  @Override
  default GeometryPipeline apply(int zoom) {
    // allow GeometryPipeline to be used where ZoomFunction<GeometryPipeline> is expected
    return this;
  }

  default GeometryPipeline andThen(GeometryPipeline other) {
    return input -> other.apply(apply(input));
  }

  /** Returns a function equivalent to {@code b(a(geom))} at each zoom. */
  static ZoomFunction<GeometryPipeline> compose(ZoomFunction<GeometryPipeline> a, ZoomFunction<GeometryPipeline> b) {
    return zoom -> ZoomFunction.applyOrElse(a, zoom, NOOP).andThen(ZoomFunction.applyOrElse(b, zoom, NOOP));
  }

  /** Returns a function that applies each argument sequentially to the input geometry at each zoom. */
  @SafeVarargs
  static ZoomFunction<GeometryPipeline> compose(
    ZoomFunction<GeometryPipeline> a,
    ZoomFunction<GeometryPipeline> b,
    ZoomFunction<GeometryPipeline> c,
    ZoomFunction<GeometryPipeline>... rest) {
    return zoom -> geom -> {
      geom = ZoomFunction.applyOrElse(a, zoom, NOOP).apply(geom);
      geom = ZoomFunction.applyOrElse(b, zoom, NOOP).apply(geom);
      geom = ZoomFunction.applyOrElse(c, zoom, NOOP).apply(geom);
      if (rest != null) {
        for (var fn : rest) {
          geom = ZoomFunction.applyOrElse(fn, zoom, NOOP).apply(geom);
        }
      }
      return geom;
    };
  }

  /** Returns a function equivalent to {@code b(a(geom))} at each zoom. */
  static ZoomFunction<GeometryPipeline> compose(ZoomFunction<GeometryPipeline> a, GeometryPipeline b) {
    return zoom -> ZoomFunction.applyOrElse(a, zoom, NOOP).andThen(b);
  }

  /** Returns a function equivalent to {@code b(a(geom))} at each zoom. */
  static ZoomFunction<GeometryPipeline> compose(GeometryPipeline a, ZoomFunction<GeometryPipeline> b) {
    return zoom -> a.andThen(ZoomFunction.applyOrElse(b, zoom, NOOP));
  }

  /** Returns a function equivalent to {@code b(a(geom))} at each zoom. */
  static GeometryPipeline compose(GeometryPipeline a, GeometryPipeline b) {
    return a.andThen(b);
  }


  /**
   * Returns a geometry pipeline that applies the normal geometry simplification to {@code feature} that would happen
   * without any geometry pipeline.
   */
  static ZoomFunction<GeometryPipeline> defaultSimplify(FeatureCollector.Feature feature) {
    return zoom -> {
      double tolerance = feature.getPixelToleranceAtZoom(zoom) / 256d;
      SimplifyMethod simplifyMethod = feature.getSimplifyMethodAtZoom(zoom);
      return geom -> switch (simplifyMethod) {
        case RETAIN_IMPORTANT_POINTS -> DouglasPeuckerSimplifier.simplify(geom, tolerance);
        // DP tolerance is displacement, and VW tolerance is area, so square what the user entered to convert from
        // DP to VW tolerance
        case RETAIN_EFFECTIVE_AREAS -> new VWSimplifier().setTolerance(tolerance * tolerance).transform(geom);
        case RETAIN_WEIGHTED_EFFECTIVE_AREAS ->
          new VWSimplifier().setWeight(0.7).setTolerance(tolerance * tolerance).transform(geom);
      };
    };
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
