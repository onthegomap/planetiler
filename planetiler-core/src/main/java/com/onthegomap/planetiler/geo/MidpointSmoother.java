package com.onthegomap.planetiler.geo;

import java.util.Arrays;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Smooths an input geometry by interpolating points along each edge and repeating for a set number of iterations.
 * <p>
 * When the points array is {@code [0.5]} this means midpoint smoothing, and when it is {@code [0.25, 0.75]} it means
 * <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin Smoothing</a>.
 */
public class MidpointSmoother extends GeometryTransformer implements GeometryPipeline {

  private static final double[] CHAIKIN = new double[]{0.25, 0.75};
  private static final double[] MIDPOINT = new double[]{0.5};
  private final double[] points;
  private int iters = 1;
  private double minSquaredVertexTolerance = 0;
  private double minVertexArea = 0;

  public MidpointSmoother(double[] points) {
    if (points.length < 1 || points.length > 2) {
      throw new IllegalArgumentException("Smoothing only works with 1 or 2 points along each edge, got %s".formatted(
        Arrays.toString(points)));
    }
    this.points = points;
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> {@code iters} times on the input line.
   */
  public static MidpointSmoother chaikin(int iters) {
    return new MidpointSmoother(CHAIKIN).setIters(iters);
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> until the added points would get dropped with {@link DouglasPeuckerSimplifier Douglas-Peucker
   * simplification} with {@code tolerance} threshold.
   */
  public static MidpointSmoother chaikinToTolerance(double tolerance) {
    return new MidpointSmoother(CHAIKIN).setIters(10).setMinVertexTolerance(tolerance);
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> until the added points would get dropped with {@link VWSimplifier Visvalingam-Whyatt simplification}
   * with {@code minArea} threshold.
   */
  public static MidpointSmoother chaikinToMinArea(double minArea) {
    return new MidpointSmoother(CHAIKIN).setIters(10).setMinVertexArea(minArea);
  }

  /**
   * Sets the min distance between a vertex and the line between its neighbors below which a vertex will not be
   * squashed.
   * <p>
   * If all points are below this threshold for an entire iteration then smoothing will stop before the maximum number
   * of iterations is reached.
   */
  public MidpointSmoother setMinVertexTolerance(double minVertexTolerance) {
    this.minSquaredVertexTolerance = minVertexTolerance * minVertexTolerance;
    return this;
  }

  /**
   * Sets the min area of the triangle created by a point and its 2 neighbors below which a vertex will not be squashed.
   * <p>
   * If all points are below this threshold for an entire iteration then smoothing will stop before the maximum number
   * of iterations is reached.
   */
  public MidpointSmoother setMinVertexArea(double minVertexArea) {
    this.minVertexArea = minVertexArea;
    return this;
  }

  /** Returns a new smoother that connects the points halfway along each edge. */
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
    if (points.length == 1) {
      return singlePointSmooth(coords, area, points[0]);
    } else if (points.length >= 2) {
      return dualPointSmooth(coords, area, points[0], points[1]);
    }
    return coords.copy();
  }

  private CoordinateSequence singlePointSmooth(CoordinateSequence coords, boolean area, double ratio) {
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

  private CoordinateSequence dualPointSmooth(CoordinateSequence coords, boolean area, double a, double b) {
    boolean checkVertices = minSquaredVertexTolerance > 0 || minVertexArea > 0;
    for (int iter = 0; iter < iters; iter++) {
      MutableCoordinateSequence result = new MutableCoordinateSequence();
      if (!area) {
        result.addPoint(coords.getX(0), coords.getY(0));
      }
      int last = coords.size() - 1;
      double x2 = coords.getX(0);
      double y2 = coords.getY(0);
      boolean skippedLastVertex = false;
      double x1, y1;
      for (int i = 0; i < last; i++) {
        x1 = x2;
        y1 = y2;
        x2 = coords.getX(i + 1);
        y2 = coords.getY(i + 1);
        double dx = x2 - x1;
        double dy = y2 - y1;

        if ((area || i > 0) && !skippedLastVertex) {
          result.addPoint(x1 + dx * a, y1 + dy * a);
        }


        if (area || i < last - 1) {
          if (checkVertices) {
            int next = i < last - 1 ? (i + 2) : 1;
            double x3 = coords.getX(next);
            double y3 = coords.getY(next);
            if (skipVertex(x1, y1, x2, y2, x3, y3)) {
              result.addPoint(x2, y2);
              skippedLastVertex = true;
              continue;
            } else {
              skippedLastVertex = false;
            }
          }

          result.addPoint(x1 + dx * b, y1 + dy * b);
        }
      }
      if (area) {
        if (skippedLastVertex) {
          result.setX(0, result.getX(result.size() - 1));
          result.setY(0, result.getY(result.size() - 1));
        } else {
          result.closeRing();
        }
      } else {
        result.addPoint(coords.getX(last), coords.getY(last));
      }
      // early exit case: if no vertices were collapsed then we are done smoothing
      if (coords.size() == result.size()) {
        return result;
      }
      coords = result;
    }
    return coords;
  }

  private boolean skipVertex(double x1, double y1, double x2, double y2, double x3, double y3) {
    return (minVertexArea > 0 && VWSimplifier.triangleArea(x1, y1, x2, y2, x3, y3) < minVertexArea) ||
      (minSquaredVertexTolerance > 0 &&
        DouglasPeuckerSimplifier.getSqSegDist(x2, y2, x1, y1, x3, y3) < minSquaredVertexTolerance);
  }
}
