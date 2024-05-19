package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

/**
 * Utilities for converting nested <a href=
 * "https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md#native-encodings-based-on-geoarrow">geoarrow</a>
 * coordinate lists to JTS geometries.
 */
class GeoArrow {
  // TODO create packed coordinate arrays while reading parquet values to avoid creating so many intermediate objects
  static MultiPolygon multipolygon(List<List<List<Object>>> list) {
    return GeoUtils.createMultiPolygon(map(list, GeoArrow::polygon));
  }

  static Polygon polygon(List<List<Object>> input) {
    return GeoUtils.createPolygon(ring(input.getFirst()), input.stream().skip(1).map(GeoArrow::ring).toList());
  }

  static MultiPoint multipoint(List<Object> input) {
    return GeoUtils.createMultiPoint(map(input, GeoArrow::point));
  }

  static Point point(Object input) {
    int dims = input instanceof List<?> l ? l.size() : input instanceof Map<?, ?> m ? m.size() : 0;
    CoordinateSequence result =
      new PackedCoordinateSequence.Double(1, dims, dims == 4 ? 1 : 0);
    coordinate(input, result, 0);
    return GeoUtils.JTS_FACTORY.createPoint(result);
  }

  static MultiLineString multilinestring(List<List<Object>> input) {
    return GeoUtils.createMultiLineString(map(input, GeoArrow::linestring));
  }

  static LineString linestring(List<Object> input) {
    return GeoUtils.JTS_FACTORY.createLineString(coordinateSequence(input));
  }


  private static CoordinateSequence coordinateSequence(List<Object> input) {
    if (input.isEmpty()) {
      return GeoUtils.EMPTY_COORDINATE_SEQUENCE;
    }
    Object first = input.getFirst();
    int dims = first instanceof List<?> l ? l.size() : first instanceof Map<?, ?> m ? m.size() : 0;
    CoordinateSequence result =
      new PackedCoordinateSequence.Double(input.size(), dims, dims == 4 ? 1 : 0);
    for (int i = 0; i < input.size(); i++) {
      Object item = input.get(i);
      coordinate(item, result, i);
    }
    return result;
  }

  private static LinearRing ring(List<Object> input) {
    return GeoUtils.JTS_FACTORY.createLinearRing(coordinateSequence(input));
  }

  private static void coordinate(Object input, CoordinateSequence result, int index) {
    switch (input) {
      case List<?> list -> {
        List<Number> l = (List<Number>) list;
        for (int i = 0; i < l.size(); i++) {
          result.setOrdinate(index, i, l.get(i).doubleValue());
        }
      }
      case Map<?, ?> map -> {
        Map<String, Number> m = (Map<String, Number>) map;

        for (var entry : m.entrySet()) {
          int ordinateIndex = switch (entry.getKey()) {
            case "x" -> 0;
            case "y" -> 1;
            case "z" -> 2;
            case "m" -> 3;
            case null, default -> throw new IllegalArgumentException("Bad coordinate key: " + entry.getKey());
          };
          result.setOrdinate(index, ordinateIndex, entry.getValue().doubleValue());
        }
      }
      default -> throw new IllegalArgumentException("Expecting map or list, got: " + input);
    }
  }

  private static <I, O> List<O> map(List<I> in, Function<I, O> remap) {
    return in.stream().map(remap).toList();
  }

}
