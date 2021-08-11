package com.onthegomap.flatmap.geo;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

@ThreadSafe
public class PointIndex<T> {

  private record GeomWithData<T>(Coordinate coord, T data) {}

  private final STRtree index = new STRtree();

  private PointIndex() {
  }

  public static <T> PointIndex<T> create() {
    return new PointIndex<>();
  }

  private volatile boolean built = false;

  private void build() {
    if (!built) {
      synchronized (this) {
        if (!built) {
          index.build();
          built = true;
        }
      }
    }
  }

  public List<T> getWithin(Point point, double threshold) {
    build();
    Coordinate coord = point.getCoordinate();
    Envelope envelope = point.getEnvelopeInternal();
    envelope.expandBy(threshold);
    List<?> items = index.query(envelope);
    List<T> result = new ArrayList<>(items.size());
    for (int i = 0; i < items.size(); i++) {
      if (items.get(i) instanceof GeomWithData<?> value) {
        double distance = value.coord.distance(coord);
        if (distance <= threshold) {
          @SuppressWarnings("unchecked") T t = (T) value.data;
          result.add(t);
        }
      }
    }
    return result;
  }

  public T getNearest(Point point, double threshold) {
    build();
    Coordinate coord = point.getCoordinate();
    Envelope envelope = point.getEnvelopeInternal();
    envelope.expandBy(threshold);
    List<?> items = index.query(envelope);
    double nearestDistance = Double.MAX_VALUE;
    T nearestValue = null;
    for (int i = 0; i < items.size(); i++) {
      if (items.get(i) instanceof GeomWithData<?> value) {
        double distance = value.coord.distance(coord);
        if (distance < nearestDistance) {
          @SuppressWarnings("unchecked") T t = (T) value.data;
          nearestDistance = distance;
          nearestValue = t;
        }
      }
    }
    return nearestValue;
  }

  public void put(Geometry geom, T item) {
    if (geom instanceof Point point && !point.isEmpty()) {
      Envelope envelope = Objects.requireNonNull(point.getEnvelopeInternal());
      // need to externally synchronize inserts into the STRTree
      synchronized (this) {
        index.insert(envelope, new GeomWithData<>(point.getCoordinate(), item));
      }
    } else if (geom instanceof GeometryCollection geoms) {
      for (int i = 0; i < geoms.getNumGeometries(); i++) {
        put(geoms.getGeometryN(i), item);
      }
    }
  }

}
