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

  public VWSimplifier() {}

  public VWSimplifier setTolerance(double tolerance) {
    this.tolerance = tolerance;
    return this;
  }

  public VWSimplifier setWeight(double k) {
    this.k = k;
    return this;
  }

  private class Vertex {
    int heapIdx;
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

    public double area() {
      if (prev == null || next == null) {
        return area = Double.POSITIVE_INFINITY;
      }
      return area = weight(prev.x, prev.y, x, y, next.x, next.y);
    }
  }

  private static class MinHeap {
    Vertex[] array;
    int length = 0;

    MinHeap(int size) {
      array = new Vertex[size + 1];
    }

    int push(Vertex vertex) {
      int idx = length++;
      vertex.heapIdx = idx;
      array[idx] = vertex;
      up(idx);
      return length;
    }

    Vertex pop() {
      var removed = array[0];
      var object = array[--length];
      array[length] = null;
      if (length > 0) {
        array[object.heapIdx = 0] = object;
        down(0);
      }
      return removed;
    }

    int remove(Vertex removed) {
      int i = removed.heapIdx;
      var object = array[--length];
      array[length] = null;
      if (i != length) {
        array[object.heapIdx = i] = object;
        if (object.area < removed.area) {
          up(i);
        } else {
          down(i);
        }
      }
      return i;
    }

    void up(int i) {
      var object = array[i];
      while (i > 0) {
        var up = ((i + 1) >> 1) - 1;
        var parent = array[up];
        if (object.area >= parent.area) {
          break;
        }
        array[parent.heapIdx = i] = parent;
        array[object.heapIdx = i = up] = object;
      }
    }

    void down(int i) {
      var object = array[i];
      while (true) {
        var right = (i + 1) << 1;
        var left = right - 1;
        var down = i;
        var child = array[down];
        if (left < length && array[left].area < child.area) {
          child = array[down = left];
        }
        if (right < length && array[right].area < child.area) {
          child = array[down = right];
        }
        if (down == i) {
          break;
        }
        array[child.heapIdx = i] = child;
        array[object.heapIdx = i = down] = object;
      }
    }
  }

  @Override
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
    boolean area = parent instanceof LinearRing;
    if (coords.size() == 0) {
      return coords;
    }

    int num = coords.size();
    //  heap = minHeap(compareArea)
    DoubleMinHeap heap = DoubleMinHeap.newArrayHeap(num, Integer::compare);
    //  maxArea = 0
    //    double maxArea = 0;
    Vertex[] points = new Vertex[num];
    //  intersecting = []
    //
    Vertex prev = null;
    for (int i = 0; i < num; i++) {
      Vertex cur = new Vertex(i, coords);
      points[i] = cur;
      if (prev != null) {
        cur.prev = prev;
        prev.next = cur;
        heap.push(prev.idx, prev.area());
      }
      prev = cur;
    }
    if (prev != null) {
      heap.push(prev.idx, prev.area());
    }

    int left = num;
    int min = area ? 4 : 2;

    while (!heap.isEmpty()) {
      var id = heap.poll();
      Vertex point = points[id];

      // TODO try minheap
      //      VW(0)	 11M	2.8M	743k	373k	3.5k
      //    VW(0.1)	 11M	3.3M	431k	171k	 474
      //      VW(1)	 11M	3.2M	300k	130k	 479
      //     VW(20)	 11M	1.5M	263k	122k	 479
      if (point.area > tolerance || left <= min) {
        break;
      }
      //      if (point.area < maxArea) {
      //        point.area = maxArea;
      //      } else {
      //        maxArea = point.area;
      //      }
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
        heap.update(point.prev.idx, point.prev.area());
      }
      if (point.next != null) {
        heap.update(point.next.idx, point.next.area());
      }
    }
    //
    //
    //    // make sure we include the first and last points even if they are closer than the simplification threshold
    //    int num = coords.size();
    //    DoubleMinHeap heap = DoubleMinHeap.newArrayHeap(num, Integer::compare);
    //    double maxArea = tolerance;
    //    //      double[] weights = new double[num];
    //    int[] prev = new int[num];
    //    int[] next = new int[num];
    //    //      System.err.println("start " + num);
    //    for (int b = 0; b < num; b++) {
    //      int a = b - 1;
    //      int c = b + 1;
    //      double weight = a < 0 || c >= num ? Double.POSITIVE_INFINITY : weight(coords, a, b, c);
    //      //        weights[b] = weight;
    //      next[b] = c;
    //      prev[b] = a;
    //      heap.push(b, weight);
    //      //        System.err.println("  push " + weight);
    //    }
    //    while (!heap.isEmpty()) {
    //      double weight = heap.peekValue();
    //      int idx = heap.poll();
    //      //        System.err.println("  poll " + weight);
    //      if (weight > tolerance) {
    //        break;
    //      }
    //      int a = prev[idx];
    //      int c = next[idx];
    //      if (a > 0) {
    //        //          System.err.println("    update before=" + weight(coords, prev[a], a, c));
    //        heap.update(a, weight(coords, prev[a], a, c));
    //      }
    //      if (c < num - 1) {
    //        // update next (remove c, update c=>weight)
    //        //          System.err.println("    update after=" + weight(coords, a, c, next[c]));
    //        heap.update(c, weight(coords, a, c, next[c]));
    //      }
    //      next[prev[idx]] = c;
    //      prev[next[idx]] = a;
    //    }
    // for polygons, additionally keep at least 2 intermediate points even if they are below simplification threshold
    // to avoid collapse.
    MutableCoordinateSequence result = new MutableCoordinateSequence();
    for (Vertex point = points[0]; point != null; point = point.next) {
      //        if (weights[i] > tolerance || (area && i < 3)) {
      result.forceAddPoint(point.x, point.y);
      //        }
    }
    //      System.err.println(result.size());
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

  private double weight(double ax, double ay, double bx, double by, double cx, double cy) {
    double area = triangleArea(ax, ay, bx, by, cx, cy);
    return k == 0 ? area : (area * weight(cos(ax, ay, bx, by, cx, cy)));
  }

  private double weight(CoordinateSequence coords, int a, int b, int c) {
    return weight(
      coords.getX(a), coords.getY(a),
      coords.getX(b), coords.getY(b),
      coords.getX(c), coords.getY(c)
    );
  }
}
