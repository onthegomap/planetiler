/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package com.onthegomap.planetiler.geo;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * A utility to simplify geometries using Douglas Peucker simplification algorithm without any attempt to repair
 * geometries that become invalid due to simplification.
 * <p>
 * This class is adapted from <a href=
 * "https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/simplify/DouglasPeuckerSimplifier.java">org.locationtech.jts.simplify.DouglasPeuckerSimplifier</a>
 * with modifications to avoid collapsing small polygons since the subsequent area filter will remove them more
 * accurately and performance improvement to put the results in a {@link MutableCoordinateSequence} which uses a
 * primitive double array instead of allocating lots of {@link Coordinate} objects.
 */
public class DouglasPeuckerSimplifier extends GeometryTransformer implements GeometryPipeline {

  private final double sqTolerance;

  public DouglasPeuckerSimplifier(double distanceTolerance) {
    this.sqTolerance = distanceTolerance * Math.abs(distanceTolerance);
  }

  @Override
  public Geometry apply(Geometry input) {
    return simplify(input);
  }

  /**
   * Returns a copy of {@code geom}, simplified using Douglas Peucker Algorithm.
   *
   * @param geom the geometry to simplify (will not be modified)
   * @return the simplified geometry
   */
  public Geometry simplify(Geometry geom) {
    if (geom.isEmpty() || (sqTolerance < 0.0)) {
      return geom.copy();
    }
    return transform(geom);
  }

  /**
   * Returns a copy of {@code geom}, simplified using Douglas Peucker Algorithm.
   *
   * @param geom              the geometry to simplify (will not be modified)
   * @param distanceTolerance the threshold below which we discard points
   * @return the simplified geometry
   */
  public static Geometry simplify(Geometry geom, double distanceTolerance) {
    if (geom.isEmpty() || (distanceTolerance < 0.0)) {
      return geom.copy();
    }

    return (new DouglasPeuckerSimplifier(distanceTolerance)).simplify(geom);
  }

  /**
   * Returns a copy of {@code coords}, simplified using Douglas Peucker Algorithm.
   *
   * @param coords            the coordinate list to simplify
   * @param distanceTolerance the threshold below which we discard points
   * @param area              true if this is a polygon to retain at least 4 points to avoid collapse
   * @return the simplified coordinate list
   */
  public static List<Coordinate> simplify(List<Coordinate> coords, double distanceTolerance, boolean area) {
    if (coords.isEmpty()) {
      return List.of();
    }

    return (new DouglasPeuckerSimplifier(distanceTolerance)).transformCoordinateList(coords, area);
  }


  /**
   * Returns the square of the number of units that (px, p1) is away from the line segment from (p1x, py1) to (p2x,
   * p2y).
   */
  private static double getSqSegDist(double px, double py, double p1x, double p1y, double p2x, double p2y) {

    double x = p1x,
      y = p1y,
      dx = p2x - x,
      dy = p2y - y;

    if (dx != 0d || dy != 0d) {

      double t = ((px - x) * dx + (py - y) * dy) / (dx * dx + dy * dy);

      if (t > 1) {
        x = p2x;
        y = p2y;

      } else if (t > 0) {
        x += dx * t;
        y += dy * t;
      }
    }

    dx = px - x;
    dy = py - y;

    return dx * dx + dy * dy;
  }

  private void subsimplify(List<Coordinate> in, List<Coordinate> out, int first, int last, int numForcedPoints) {
    // numForcePoints lets us keep some points even if they are below simplification threshold
    boolean force = numForcedPoints > 0;
    double maxSqDist = force ? -1 : sqTolerance;
    int index = -1;
    Coordinate p1 = in.get(first);
    Coordinate p2 = in.get(last);
    double p1x = p1.x;
    double p1y = p1.y;
    double p2x = p2.x;
    double p2y = p2.y;

    int i = first + 1;
    Coordinate furthest = null;
    for (Coordinate coord : in.subList(first + 1, last)) {
      double sqDist = getSqSegDist(coord.x, coord.y, p1x, p1y, p2x, p2y);

      if (sqDist > maxSqDist) {
        index = i;
        furthest = coord;
        maxSqDist = sqDist;
      }
      i++;
    }

    if (force || maxSqDist > sqTolerance) {
      if (index - first > 1) {
        subsimplify(in, out, first, index, numForcedPoints - 1);
      }
      out.add(furthest);
      if (last - index > 1) {
        subsimplify(in, out, index, last, numForcedPoints - 2);
      }
    }
  }

  private void subsimplify(CoordinateSequence in, MutableCoordinateSequence out, int first, int last,
    int numForcedPoints) {
    // numForcePoints lets us keep some points even if they are below simplification threshold
    boolean force = numForcedPoints > 0;
    double maxSqDist = force ? -1 : sqTolerance;
    int index = -1;
    double p1x = in.getX(first);
    double p1y = in.getY(first);
    double p2x = in.getX(last);
    double p2y = in.getY(last);

    for (int i = first + 1; i < last; i++) {
      double px = in.getX(i);
      double py = in.getY(i);
      double sqDist = getSqSegDist(px, py, p1x, p1y, p2x, p2y);

      if (sqDist > maxSqDist) {
        index = i;
        maxSqDist = sqDist;
      }
    }

    if (force || maxSqDist > sqTolerance) {
      if (index - first > 1) {
        subsimplify(in, out, first, index, numForcedPoints - 1);
      }
      out.forceAddPoint(in.getX(index), in.getY(index));
      if (last - index > 1) {
        subsimplify(in, out, index, last, numForcedPoints - 2);
      }
    }
  }

  protected List<Coordinate> transformCoordinateList(List<Coordinate> coords, boolean area) {
    if (coords.isEmpty()) {
      return coords;
    }
    // make sure we include the first and last points even if they are closer than the simplification threshold
    List<Coordinate> result = new ArrayList<>();
    result.add(coords.getFirst());
    // for polygons, additionally keep at least 2 intermediate points even if they are below simplification threshold
    // to avoid collapse.
    subsimplify(coords, result, 0, coords.size() - 1, area ? 2 : 0);
    result.add(coords.getLast());
    return result;
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    boolean area = parent instanceof LinearRing;
    if (coords.size() == 0) {
      return coords;
    }
    // make sure we include the first and last points even if they are closer than the simplification threshold
    MutableCoordinateSequence result = new MutableCoordinateSequence();
    result.forceAddPoint(coords.getX(0), coords.getY(0));
    // for polygons, additionally keep at least 2 intermediate points even if they are below simplification threshold
    // to avoid collapse.
    subsimplify(coords, result, 0, coords.size() - 1, area ? 2 : 0);
    result.forceAddPoint(coords.getX(coords.size() - 1), coords.getY(coords.size() - 1));
    return result;
  }

  @Override
  protected Geometry transformPolygon(Polygon geom, Geometry parent) {
    return geom.isEmpty() ? null : super.transformPolygon(geom, parent);
  }

  @Override
  protected Geometry transformLinearRing(LinearRing geom, Geometry parent) {
    boolean removeDegenerateRings = parent instanceof Polygon;
    Geometry simpResult = super.transformLinearRing(geom, parent);
    if (removeDegenerateRings && !(simpResult instanceof LinearRing)) {
      return null;
    }
    return simpResult;
  }
}
