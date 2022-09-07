package com.onthegomap.planetiler.reader;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Something attached to a geometry that can be matched using a
 * {@link com.onthegomap.planetiler.expression.Expression.MatchType} geometry type filter expression.
 */
public interface WithGeometryType {

  /** Returns true if this feature can be interpreted as a {@link Point} or {@link MultiPoint}. */
  boolean isPoint();

  /**
   * Returns true if this feature can be interpreted as a {@link Polygon} or {@link MultiPolygon}.
   * <p>
   * A closed ring can either be a polygon or linestring, so return false to not allow this closed ring to be treated as
   * a polygon.
   */
  boolean canBePolygon();

  /**
   * Returns true if this feature can be interpreted as a {@link LineString} or {@link MultiLineString}.
   * <p>
   * A closed ring can either be a polygon or linestring, so return false to not allow this closed ring to be treated as
   * a linestring.
   */
  boolean canBeLine();
}
