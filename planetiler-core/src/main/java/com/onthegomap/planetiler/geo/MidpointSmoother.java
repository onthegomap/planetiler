package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Smoothes an input geometry by interpolating points along each edge and repeating for a set number of iterations.
 * <p>
 * When the points array is {@code [0.5]} this means midpoint smoothing, and when it is {@code [0.25, 0.75]} it means
 * <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin Smoothing</a>.
 */
public class MidpointSmoother extends GeometryTransformer implements GeometryPipeline {

  private static final double[] CHAIKIN = new double[]{0.25, 0.75};
  private static final double[] MIDPOINT = new double[]{0.5};
  private final double[] points;
  private int iters = 1;

  public MidpointSmoother(double[] points) {
    this.points = points;
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> {@code iters} times on the input line.
   */
  public static MidpointSmoother chaikin(int iters) {
    return new MidpointSmoother(CHAIKIN).setIters(iters);
  }

  /** Returns a new smoother that does midpoint smoothing. */
  public static MidpointSmoother midpoint() {
    return new MidpointSmoother(MIDPOINT);
  }

  /** Sets the number of times that smoothing runs. */
  public MidpointSmoother setIters(int iters) {
    this.iters = iters;
    return this;
  }

  @Override
  public Geometry apply(Geometry input) {
    return transform(input);
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    boolean area = parent instanceof LinearRing;
    if (coords.size() <= 2) {
      return coords.copy();
    }
    for (int iter = 0; iter < iters; iter++) {
      MutableCoordinateSequence result = new MutableCoordinateSequence();
      boolean skipFirstAndLastInterpolated = !area && points.length > 1;
      if (!area) {
        result.addPoint(coords.getX(0), coords.getY(0));
      }
      int last = coords.size() - 1;
      double x2 = coords.getX(0);
      double y2 = coords.getY(0);
      double x1, y1;
      for (int i = 0; i < last; i++) {
        x1 = x2;
        y1 = y2;
        x2 = coords.getX(i + 1);
        y2 = coords.getY(i + 1);
        double dx = x2 - x1;
        double dy = y2 - y1;
        for (int j = 0; j < points.length; j++) {
          if (skipFirstAndLastInterpolated && ((i == 0 && j == 0) || (i == last - 1 && j == points.length - 1))) {
            continue;
          }

          double value = points[j];
          result.addPoint(x1 + dx * value, y1 + dy * value);
        }
      }
      if (area) {
        result.closeRing();
      } else {
        result.addPoint(coords.getX(last), coords.getY(last));
      }
      coords = result;
    }
    return coords;
  }
}
