package com.onthegomap.planetiler.geo;

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

/**
 * Index to efficiently query points within a radius from a point.
 *
 * <p>Writes and reads are thread-safe, but all writes must occur before reads.
 *
 * @param <T> the type of value associated with each point
 */
@ThreadSafe
public class PointIndex<T> {

  private record GeomWithData<T>(Coordinate coord, T data) {}

  private final STRtree index = new STRtree();

  private PointIndex() {}

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

  /** Returns the data associated with all indexed points within a radius from {@code point}. */
  public List<T> getWithin(Point point, double threshold) {
    build();
    Coordinate coord = point.getCoordinate();
    // pre-filter by rectangular envelope
    Envelope envelope = point.getEnvelopeInternal();
    envelope.expandBy(threshold);
    List<?> items = index.query(envelope);
    List<T> result = new ArrayList<>(items.size());
    // then post-filter by circular radius
    for (Object item : items) {
      if (item instanceof GeomWithData<?> value) {
        double distance = value.coord.distance(coord);
        if (distance <= threshold) {
          @SuppressWarnings("unchecked")
          T t = (T) value.data;
          result.add(t);
        }
      }
    }
    return result;
  }

  /**
   * Returns the data associated with the nearest indexed point to {@code point}, up to a certain
   * distance.
   */
  public T getNearest(Point point, double threshold) {
    build();
    Coordinate coord = point.getCoordinate();
    Envelope envelope = point.getEnvelopeInternal();
    envelope.expandBy(threshold);
    List<?> items = index.query(envelope);
    double nearestDistance = Double.MAX_VALUE;
    T nearestValue = null;
    for (Object item : items) {
      if (item instanceof GeomWithData<?> value) {
        double distance = value.coord.distance(coord);
        if (distance < nearestDistance) {
          @SuppressWarnings("unchecked")
          T t = (T) value.data;
          nearestDistance = distance;
          nearestValue = t;
        }
      }
    }
    return nearestValue;
  }

  /** Indexes {@code item} for points contained in {@code geom}. */
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
