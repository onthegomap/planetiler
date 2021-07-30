package com.onthegomap.flatmap.geo;

import com.onthegomap.flatmap.collections.MutableCoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class DouglasPeuckerSimplifier {

  public static Geometry simplify(Geometry geom, double distanceTolerance) {
    if (geom.isEmpty()) {
      return geom.copy();
    }

    return (new DPTransformer(distanceTolerance)).transform(geom);
  }

  private static class DPTransformer extends GeometryTransformer {

    private final double sqTolerance;

    public DPTransformer(double distanceTolerance) {
      this.sqTolerance = distanceTolerance * distanceTolerance;
    }

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

    private void subsimplify(CoordinateSequence in, MutableCoordinateSequence out, int first, int last, int nforce) {
      boolean force = nforce > 0;
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
          subsimplify(in, out, first, index, nforce - 1);
        }
        out.forceAddPoint(in.getX(index), in.getY(index));
        if (last - index > 1) {
          subsimplify(in, out, index, last, nforce - 2);
        }
      }
    }

    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      boolean area = parent instanceof LinearRing;
      if (coords.size() == 0) {
        return coords;
      }
      MutableCoordinateSequence result = new MutableCoordinateSequence();
      result.forceAddPoint(coords.getX(0), coords.getY(0));
      subsimplify(coords, result, 0, coords.size() - 1, area ? 2 : 0);
      result.forceAddPoint(coords.getX(coords.size() - 1), coords.getY(coords.size() - 1));
      return result;
    }

    /**
     * Simplifies a polygon, fixing it if required.
     */
    protected Geometry transformPolygon(Polygon geom, Geometry parent) {
      // empty geometries are simply removed
      if (geom.isEmpty()) {
        return null;
      }
      return super.transformPolygon(geom, parent);
    }

    /**
     * Simplifies a LinearRing.  If the simplification results in a degenerate ring, remove the component.
     *
     * @return null if the simplification results in a degenerate ring
     */
    protected Geometry transformLinearRing(LinearRing geom, Geometry parent) {
      boolean removeDegenerateRings = parent instanceof Polygon;
      Geometry simpResult = super.transformLinearRing(geom, parent);
      if (removeDegenerateRings && !(simpResult instanceof LinearRing)) {
        return null;
      }
      return simpResult;
    }
  }
}

