package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Smooths an input geometry by interpolating 2 points at certain ratios along each edge and repeating for a set number
 * of iterations. This can be thought of as slicing off each vertex until the segments are so short it appears round.
 * <p>
 * Instead of iterating a fixed number of iterations, you can set {@link #setMinVertexArea(double)} or
 * {@link #setMinVertexOffset(double)} to stop smoothing corners when the triangle formed by 3 consecutive vertices gets
 * to small.
 * <p>
 * In order to avoid introducing too much error from the original shape when rounding corners between very long edges,
 * you can set {@link #setMaxVertexArea(double)} or {@link #setMaxVertexOffset(double)} to move the new points closer to
 * a vertex to limit the amount of error that is introduced.
 * <p>
 * When the points are {@code [0.25, 0.75]} this is equivalent to
 * <a href="https://observablehq.com/@pamacha/chaikins-algorithm">Chaikin Smoothing</a>.
 */
public class DualMidpointSmoother extends GeometryTransformer implements GeometryPipeline {

  private final double a;
  private final double b;
  private int iters = 1;
  private double minVertexArea = 0;
  private double minSquaredVertexOffset = 0;
  private double maxVertexArea = 0;
  private double maxSquaredVertexOffset = 0;

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
    return chaikin(10).setMinVertexOffset(tolerance);
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
  public DualMidpointSmoother setMinVertexOffset(double minVertexOffset) {
    this.minSquaredVertexOffset = minVertexOffset * Math.abs(minVertexOffset);
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
  public DualMidpointSmoother setMaxVertexArea(double maxArea) {
    this.maxVertexArea = maxArea;
    return this;
  }

  /**
   * Sets a limit on the maximum distance from the original shape that can be introduced by smoothing. When the error
   * introduced by squashing a vertex will be above this threshold, the new points are moved closer to the vertex in
   * order to bring the distance down to this threshold.
   * <p>
   * This prevents smoothing 2 long adjacent edges from introducing a large deviation from the original shape.
   */
  public DualMidpointSmoother setMaxVertexOffset(double maxVertexOffset) {
    this.maxSquaredVertexOffset = maxVertexOffset * Math.abs(maxVertexOffset);
    return this;
  }

  /** Sets the maximum number of times that smoothing runs. */
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
    for (int iter = 0; iter < iters; iter++) {
      assert iter < iters - 1 || (minSquaredVertexOffset == 0 && minVertexArea == 0) : "reached max iterations";
      MutableCoordinateSequence result = new MutableCoordinateSequence();
      int last = coords.size() - 1;
      double x1, y1;
      double x2 = coords.getX(0), y2 = coords.getY(0);
      double x3 = coords.getX(1), y3 = coords.getY(1);
      // for lines, always add the first point but for polygons this is a placeholder that will be updated
      // when last vertex is squashed
      result.addPoint(x2, y2);
      for (int i = 1; i < last; i++) {
        x1 = x2;
        y1 = y2;
        x2 = x3;
        y2 = y3;
        x3 = coords.getX(i + 1);
        y3 = coords.getY(i + 1);
        squashVertex(result, x1, y1, x2, y2, x3, y3);
      }
      if (area) {
        squashVertex(result,
          coords.getX(last - 1),
          coords.getY(last - 1),
          coords.getX(0),
          coords.getY(0),
          coords.getX(1),
          coords.getY(1)
        );
        int idx = result.size() - 1;
        result.setX(0, result.getX(idx));
        result.setY(0, result.getY(idx));
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

  private void squashVertex(MutableCoordinateSequence result, double x1, double y1, double x2, double y2, double x3,
    double y3) {

    if (skipVertex(x1, y1, x2, y2, x3, y3)) {
      result.addPoint(x2, y2);
      return;
    }

    double nextB = b;
    double nextA = a;

    // check the amount of error introduced by removing this vertex (either by offset or area)
    // and if it is too large, then move nextA/nextB ratios closer to the vertex to limit the error
    if (maxVertexArea > 0 || maxSquaredVertexOffset > 0) {
      double magA = Math.hypot(x2 - x1, y2 - y1);
      double magB = Math.hypot(x3 - x2, y3 - y2);
      double den = magA * magB;
      double aDist = magA * (1 - b);
      double bDist = magB * a;
      double maxDistSquared = Double.POSITIVE_INFINITY;
      if (maxVertexArea > 0) {
        double sin = den <= 0 ? 0 : Math.abs(((x1 - x2) * (y3 - y2)) - ((y1 - y2) * (x3 - x2))) / den;
        if (sin != 0) {
          maxDistSquared = 2 * maxVertexArea / sin;
        }
      }
      if (maxSquaredVertexOffset > 0) {
        double cos = den <= 0 ? 0 : Math.clamp(((x1 - x2) * (x3 - x2) + (y1 - y2) * (y3 - y2)) / den, -1, 1);
        maxDistSquared = Math.min(maxDistSquared, 2 * maxSquaredVertexOffset / (1 + cos));
      }
      double maxDist = Double.NaN;
      if (aDist * aDist > maxDistSquared) {
        nextB = 1 - (maxDist = Math.sqrt(maxDistSquared)) / magA;
      }
      if (bDist * bDist > maxDistSquared) {
        if (Double.isNaN(maxDist)) {
          maxDist = Math.sqrt(maxDistSquared);
        }
        nextA = maxDist / magB;
      }
    }

    result.addPoint(x1 + (x2 - x1) * nextB, y1 + (y2 - y1) * nextB);
    result.addPoint(x2 + (x3 - x2) * nextA, y2 + (y3 - y2) * nextA);
  }

  private boolean skipVertex(double x1, double y1, double x2, double y2, double x3, double y3) {
    return (minVertexArea > 0 && VWSimplifier.triangleArea(x1, y1, x2, y2, x3, y3) < minVertexArea) ||
      (minSquaredVertexOffset > 0 &&
        DouglasPeuckerSimplifier.getSqSegDist(x2, y2, x1, y1, x3, y3) < minSquaredVertexOffset);
  }
}
