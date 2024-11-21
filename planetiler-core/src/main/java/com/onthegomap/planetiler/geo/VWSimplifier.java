package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.collection.DoubleMinHeap;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class VWSimplifier extends GeometryTransformer {

  private double tolerance;
  private double k;

  public VWSimplifier setTolerance(double tolerance) {
    this.tolerance = tolerance;
    return this;
  }

  public VWSimplifier setWeight(double k) {
    this.k = k;
    return this;
  }

  private class Vertex {
    int idx;
    double x;
    double y;
    double area;
    Vertex prev;
    Vertex next;

    Vertex(int idx, CoordinateSequence seq) {
      this.idx = idx;
      this.x = seq.getX(idx);
      this.y = seq.getY(idx);
    }

    public void remove() {
      if (prev != null) {
        prev.next = next;
      }
      if (next != null) {
        next.prev = prev;
      }
    }

    public double updateArea() {
      if (prev == null || next == null) {
        return area = Double.POSITIVE_INFINITY;
      }
      return area = weightedArea(prev.x, prev.y, x, y, next.x, next.y);
    }
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    boolean area = parent instanceof LinearRing;
    if (coords.size() == 0) {
      return coords;
    }

    int num = coords.size();
    DoubleMinHeap heap = DoubleMinHeap.newArrayHeap(num, Integer::compare);
    Vertex[] points = new Vertex[num];
    //    Stack<Vertex> intersecting = new Stack<>();
    Vertex prev = null;
    for (int i = 0; i < num; i++) {
      Vertex cur = new Vertex(i, coords);
      points[i] = cur;
      if (prev != null) {
        cur.prev = prev;
        prev.next = cur;
        heap.push(prev.idx, prev.updateArea());
      }
      prev = cur;
    }
    if (prev != null) {
      heap.push(prev.idx, prev.updateArea());
    }

    int left = num;
    int min = area ? 4 : 2;

    while (!heap.isEmpty()) {
      var id = heap.poll();
      Vertex point = points[id];

      if (point.area > tolerance || left <= min) {
        break;
      }
      //    // Check that the new segment doesn’t intersect with
      //    // any existing segments, except for the point’s
      //    // immediate neighbours.
      //    if (intersect(heap, point.previous, point.next))
      //      intersecting.push(point);
      //      continue
      //    // Reattempt to process points that previously would
      //    // have caused intersections when removed.
      //    while (i = intersecting.pop()) heap.push(i)

      point.remove();
      left--;
      if (point.prev != null) {
        heap.update(point.prev.idx, point.prev.updateArea());
      }
      if (point.next != null) {
        heap.update(point.next.idx, point.next.updateArea());
      }
    }
    MutableCoordinateSequence result = new MutableCoordinateSequence();
    for (Vertex point = points[0]; point != null; point = point.next) {
      result.forceAddPoint(point.x, point.y);
    }
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

  private static double triangleArea(double ax, double ay, double bx, double by, double cx, double cy) {
    return Math.abs(((ay - cy) * (bx - cx) + (by - cy) * (cx - ax)) / 2);
  }

  private static double cos(double ax, double ay, double bx, double by, double cx, double cy) {
    double den = Math.hypot(bx - ax, by - ay) * Math.hypot(cx - bx, cy - by),
      cos = 0;
    if (den > 0) {
      cos = Math.clamp((ax - bx) * (cx - bx) + (ay - by) * (cy - by) / den, -1, 1);
    }
    return cos;
  }

  private double weight(double cos) {
    return (-cos) * k + 1;
  }

  private double weightedArea(double ax, double ay, double bx, double by, double cx, double cy) {
    double area = triangleArea(ax, ay, bx, by, cx, cy);
    return k == 0 ? area : (area * weight(cos(ax, ay, bx, by, cx, cy)));
  }
}
