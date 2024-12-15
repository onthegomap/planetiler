package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Smooths an input geometry by iteratively joining the midpoint of each edge of lines or polygons in the order in which
 * they are encountered.
 *
 * @see <a href="https://github.com/wipfli/midpoint-smoothing">github.com/wipfli/midpoint-smoothing</a>.
 */
public class MidpointSmoother extends GeometryTransformer implements GeometryPipeline {

  private final double ratio;
  private int iters = 1;

  public MidpointSmoother(double ratio) {
    this.ratio = ratio;
  }

  public MidpointSmoother() {
    this(0.5);
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
        result.addPoint(x1 + (x2 - x1) * ratio, y1 + (y2 - y1) * ratio);
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
