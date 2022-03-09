package com.onthegomap.planetiler.geo;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

/**
 * Index to efficiently query which polygons contain a point.
 * <p>
 * Writes and reads are thread-safe, but all writes must occur before reads.
 *
 * @param <T> the type of value associated with each polygon
 */
@ThreadSafe
public class PolygonIndex<T> {

  private record GeomWithData<T> (Polygon poly, T data) {}

  private final STRtree index = new STRtree();

  private PolygonIndex() {}

  public static <T> PolygonIndex<T> create() {
    return new PolygonIndex<>();
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

  /** Returns the data associated with the first polygon containing {@code point}. */
  public T getOnlyContaining(Point point) {
    List<T> result = getContaining(point);
    return result.isEmpty() ? null : result.get(0);
  }

  /** Returns the data associated with all polygons containing {@code point}. */
  public List<T> getContaining(Point point) {
    build();
    // first pre-filter polygons with envelope that overlaps this point
    List<?> items = index.query(point.getEnvelopeInternal());
    // then post-filter to only polygons that actually contain the point
    return postFilterContaining(point, items);
  }

  private List<T> postFilterContaining(Point point, List<?> items) {
    List<T> result = new ArrayList<>(items.size());
    for (Object item : items) {
      if (item instanceof GeomWithData<?> value && value.poly.contains(point)) {
        @SuppressWarnings("unchecked") T t = (T) value.data;
        result.add(t);
      }
    }
    return result;
  }

  /**
   * Returns the data associated with either the polygons that contain {@code point} or if none are found than the
   * nearest polygon to {@code point} with an envelope that contains point.
   */
  public List<T> getContainingOrNearest(Point point) {
    build();
    List<?> items = index.query(point.getEnvelopeInternal());
    // optimization: if there's only one then skip checking contains/distance
    if (items.size() == 1) {
      if (items.get(0)instanceof GeomWithData<?> value) {
        @SuppressWarnings("unchecked") T t = (T) value.data;
        return List.of(t);
      }
    }
    List<T> result = postFilterContaining(point, items);

    // if none contain, then look for the nearest polygon from potential overlaps
    if (result.isEmpty()) {
      double nearest = Double.MAX_VALUE;
      T nearestValue = null;
      for (Object item : items) {
        if (item instanceof GeomWithData<?> value) {
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

  /** Returns the data associated with a polygon that contains {@code point} or nearest polygon if none are found. */
  public T get(Point point) {
    List<T> nearests = getContainingOrNearest(point);
    return nearests.isEmpty() ? null : nearests.get(0);
  }

  /** Indexes {@code item} for all polygons contained in {@code geom}. */
  public void put(Geometry geom, T item) {
    if (geom instanceof Polygon poly) {
      // need to externally synchronize inserts into the STRTree
      synchronized (this) {
        index.insert(poly.getEnvelopeInternal(), new GeomWithData<>(poly, item));
      }
    } else if (geom instanceof GeometryCollection geoms) {
      for (int i = 0; i < geoms.getNumGeometries(); i++) {
        put(geoms.getGeometryN(i), item);
      }
    }
  }
}
