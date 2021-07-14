package com.onthegomap.flatmap.geo;

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

public class PolygonIndex<T> {

  private record GeomWithData<T>(Polygon poly, T data) {}

  private final STRtree index = new STRtree();

  private PolygonIndex() {
  }

  public static <T> PolygonIndex<T> create() {
    return new PolygonIndex<>();
  }

  public List<T> getContaining(Point point) {
    List<?> items = index.query(point.getEnvelopeInternal());
    return getContaining(point, items);
  }

  public T getOnlyContaining(Point point) {
    List<T> result = getContaining(point);
    return result.isEmpty() ? null : result.get(0);
  }

  @NotNull
  private List<T> getContaining(Point point, List<?> items) {
    List<T> result = new ArrayList<>(items.size());
    for (int i = 0; i < items.size(); i++) {
      if (items.get(i) instanceof GeomWithData<?> value && value.poly.contains(point)) {
        @SuppressWarnings("unchecked") T t = (T) value.data;
        result.add(t);
      }
    }
    return result;
  }

  public List<T> getContainingOrNearest(Point point) {
    List<?> items = index.query(point.getEnvelopeInternal());
    // optimization: if there's only one then skip checking contains/distance
    if (items.size() == 1) {
      if (items.get(0) instanceof GeomWithData<?> value) {
        @SuppressWarnings("unchecked") T t = (T) value.data;
        return List.of(t);
      }
    }
    List<T> result = getContaining(point, items);
    if (result.isEmpty()) {
      double nearest = Double.MAX_VALUE;
      T nearestValue = null;
      for (int i = 0; i < items.size(); i++) {
        if (items.get(i) instanceof GeomWithData<?> value) {
          double distance = value.poly.distance(point);
          if (distance < nearest) {
            @SuppressWarnings("unchecked") T t = (T) value.data;
            nearestValue = t;
            nearest = distance;
          }
        }
      }
      if (nearestValue != null) {
        result.add(nearestValue);
      }
    }
    return result;
  }

  public T get(Point p) {
    List<T> nearests = getContainingOrNearest(p);
    return nearests.isEmpty() ? null : nearests.get(0);
  }

  public synchronized void put(Geometry geom, T item) {
    if (geom instanceof Polygon poly) {
      index.insert(poly.getEnvelopeInternal(), new GeomWithData<>(poly, item));
    } else if (geom instanceof GeometryCollection geoms) {
      for (int i = 0; i < geoms.getNumGeometries(); i++) {
        put(geoms.getGeometryN(i), item);
      }
    }
  }
}
