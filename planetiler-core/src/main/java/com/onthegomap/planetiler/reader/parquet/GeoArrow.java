package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import java.util.function.Function;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Utilities for converting nested <a href=
 * "https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md#native-encodings-based-on-geoarrow">geoarrow</a>
 * coordinate lists to JTS geometries.
 */
class GeoArrow {
  private GeoArrow() {}

  static MultiPolygon multipolygon(List<List<CoordinateSequence>> list) {
    return GeoUtils.createMultiPolygon(map(list, GeoArrow::polygon));
  }

  static Polygon polygon(List<CoordinateSequence> input) {
    return GeoUtils.createPolygon(ring(input.getFirst()), input.stream().skip(1).map(GeoArrow::ring).toList());
  }

  static MultiPoint multipoint(List<CoordinateSequence> input) {
    return GeoUtils.createMultiPoint(map(input, GeoArrow::point));
  }

  static Point point(CoordinateSequence input) {
    return GeoUtils.JTS_FACTORY.createPoint(input);
  }

  static MultiLineString multilinestring(List<CoordinateSequence> input) {
    return GeoUtils.createMultiLineString(map(input, GeoArrow::linestring));
  }

  static LineString linestring(CoordinateSequence input) {
    return GeoUtils.JTS_FACTORY.createLineString(input);
  }

  private static LinearRing ring(CoordinateSequence input) {
    return GeoUtils.JTS_FACTORY.createLinearRing(input);
  }

  private static <I, O> List<O> map(List<I> in, Function<I, O> remap) {
    return in.stream().map(remap).toList();
  }

}
