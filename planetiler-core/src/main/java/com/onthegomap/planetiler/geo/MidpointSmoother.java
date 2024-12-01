package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class MidpointSmoother extends GeometryTransformer implements GeometryPipeline {

  private final double[] points;
  private int iters = 1;
  private boolean includeLineEndpoints = true;

  public MidpointSmoother(double[] points) {
    this.points = points;
  }

  @Override
  public Geometry apply(Geometry input) {
    return transform(input);
  }

  public MidpointSmoother setIters(int iters) {
    this.iters = iters;
    return this;
  }

  public MidpointSmoother includeLineEndpoints(boolean includeLineEndpoints) {
    this.includeLineEndpoints = includeLineEndpoints;
    return this;
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    boolean area = parent instanceof LinearRing;
    if (coords.size() <= 2) {
      return coords.copy();
    }
    for (int iter = 0; iter < iters; iter++) {
      MutableCoordinateSequence result = new MutableCoordinateSequence();
      boolean skipFirstAndLastInterpolated = !area && includeLineEndpoints && points.length > 1;
      if (!area && includeLineEndpoints) {
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
          if (skipFirstAndLastInterpolated) {
            if ((i == 0 && j == 0) || (i == last - 1 && j == points.length - 1)) {
              continue;
            }
          }
          double value = points[j];
          result.addPoint(x1 + dx * value, y1 + dy * value);
        }
      }
      if (area) {
        result.closeRing();
      } else if (includeLineEndpoints) {
        result.addPoint(coords.getX(last), coords.getY(last));
      }
      coords = result;
    }
    return coords;
  }
}
