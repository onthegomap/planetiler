package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Smooths an input geometry by interpolating 2 points at certain ratios along each edge and repeating for a set number
 * of iterations. This can be thought of as slicing off of each vertex until the segments are so short it appears round.
 * <p>
 * When the points are {@code [0.25, 0.75]} this is equivalent to
 * <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin Smoothing</a>.
 */
public class DualMidpointSmoother extends GeometryTransformer implements GeometryPipeline {

  private final double a;
  private final double b;
  private int iters = 1;
  private double minSquaredVertexTolerance = 0;
  private double minVertexArea = 0;
  private double maxArea = 0;
  private double maxSquaredOffset = 0;

  public DualMidpointSmoother(double a, double b) {
    this.a = a;
    this.b = b;
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> {@code iters} times on the input line.
   */
  public static DualMidpointSmoother chaikin(int iters) {
    return new DualMidpointSmoother(0.25, 0.75).setIters(iters);
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> but instead of stopping after a fixed number of iterations, it stops when the added points would get
   * dropped with {@link DouglasPeuckerSimplifier Douglas-Peucker simplification} with {@code tolerance} threshold.
   */
  public static DualMidpointSmoother chaikinToTolerance(double tolerance) {
    return chaikin(10).setMinVertexTolerance(tolerance);
  }

  /**
   * Returns a new smoother that does <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin
   * Smoothing</a> but instead of stopping after a fixed number of iterations, it stops when the added points would get
   * dropped with {@link VWSimplifier Visvalingam-Whyatt simplification} with {@code minArea} threshold.
   */
  public static DualMidpointSmoother chaikinToMinArea(double minArea) {
    return chaikin(10).setMinVertexArea(minArea);
  }

  /**
   * Sets the min distance between a vertex and the line between its neighbors below which a vertex will not be
   * squashed.
   * <p>
   * If all points are below this threshold for an entire iteration then smoothing will stop before the maximum number
   * of iterations is reached.
   */
  public DualMidpointSmoother setMinVertexTolerance(double minVertexTolerance) {
    this.minSquaredVertexTolerance = minVertexTolerance * minVertexTolerance;
    return this;
  }

  /**
   * Sets the min area of the triangle created by a point and its 2 neighbors below which a vertex will not be squashed.
   * <p>
   * If all points are below this threshold for an entire iteration then smoothing will stop before the maximum number
   * of iterations is reached.
   */
  public DualMidpointSmoother setMinVertexArea(double minVertexArea) {
    this.minVertexArea = minVertexArea;
    return this;
  }

  /**
   * Sets a limit on the maximum area that can be removed when removing a vertex. When the area is larger than this, the
   * new points are moved closer to the vertex in order to bring the removed area down to this threshold.
   * <p>
   * This prevents smoothing 2 long adjacent edges from introducing a large deviation from the original shape.
   */
  public DualMidpointSmoother setMaxArea(double maxArea) {
    this.maxArea = maxArea;
    return this;
  }

  /**
   * Sets a limit on the maximum distance from the original shape that can be introduced by smoothing. When the error
   * introduced by squashing a vertex will be above this threshold, the new points are moved closer to the vertex in
   * order to bring the distance down to this threshold.
   * <p>
   * This prevents smoothing 2 long adjacent edges from introducing a large deviation from the original shape.
   */
  public DualMidpointSmoother setMaxOffset(double maxOffset) {
    this.maxSquaredOffset = maxOffset * maxOffset;
    return this;
  }

  /** Sets the number of times that smoothing runs. */
  public DualMidpointSmoother setIters(int iters) {
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
    boolean checkVertices = minSquaredVertexTolerance > 0 || minVertexArea > 0 || maxSquaredOffset > 0 || maxArea > 0;
    for (int iter = 0; iter < iters; iter++) {
      MutableCoordinateSequence result = new MutableCoordinateSequence();
      if (!area) {
        result.addPoint(coords.getX(0), coords.getY(0));
      }
      int last = coords.size() - 1;
      double x2 = coords.getX(0);
      double y2 = coords.getY(0);
      double nextA = a;
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
          result.addPoint(x1 + dx * nextA, y1 + dy * nextA);
        }
        nextA = a;

        if (area || i < last - 1) {
          double nextB = b;
          if (checkVertices) {
            int next = i < last - 1 ? (i + 2) : 1;
            double x3 = coords.getX(next);
            double y3 = coords.getY(next);
            if (skipVertex(x1, y1, x2, y2, x3, y3)) {
              result.addPoint(x2, y2);
              skippedLastVertex = true;
              continue;
            }
            skippedLastVertex = false;

            // check the amount of error introduced by removing this vertex (either by offset or area)
            // and if it is too large, then move nextA/nextB ratios closer to the vertex to limit the error
            if (maxArea > 0 || maxSquaredOffset > 0) {
              double magA = Math.hypot(x2 - x1, y2 - y1);
              double magB = Math.hypot(x3 - x2, y3 - y2);
              double den = magA * magB;
              double aDist = magA * (1 - b);
              double bDist = magB * a;
              double maxDistSquared = Double.POSITIVE_INFINITY;
              if (maxArea > 0) {
                double sin = den <= 0 ? 0 : Math.abs(((x1 - x2) * (y3 - y2)) - ((y1 - y2) * (x3 - x2))) / den;
                maxDistSquared = 2 * maxArea / sin;
              }
              if (maxSquaredOffset > 0) {
                double cos = den <= 0 ? 0 : Math.clamp(((x1 - x2) * (x3 - x2) + (y1 - y2) * (y3 - y2)) / den, -1, 1);
                maxDistSquared = Math.min(maxDistSquared, 2 * maxSquaredOffset / (1 + cos));
              }
              double maxDist = Double.NaN;
              if (aDist * aDist > maxDistSquared) {
                nextB = 1 - (maxDist = Math.sqrt(maxDistSquared)) / magA;
              }
              if (bDist * bDist > maxDistSquared) {
                if (Double.isNaN(maxDist)) {
                  maxDist = Math.sqrt(maxDistSquared);
                }
                nextA = maxDist / magA;
              }
            }
          }

          result.addPoint(x1 + dx * nextB, y1 + dy * nextB);
        }
      }
      if (area) {
        if (skippedLastVertex) {
          result.setX(0, result.getX(result.size() - 1));
          result.setY(0, result.getY(result.size() - 1));
        } else {
          if (nextA != a) {
            result.setX(0, (coords.getX(1) - coords.getX(0)) * nextA);
            result.setY(0, (coords.getY(1) - coords.getY(0)) * nextA);
          }
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
