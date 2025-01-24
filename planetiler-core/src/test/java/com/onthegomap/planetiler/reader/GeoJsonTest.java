package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class GeoJsonTest {
  @Test
  void testPoint() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]}}
      """, List.of(
      new GeoJsonFeature(newPoint(1, 2), Map.of())
    ));
  }

  @Test
  void testLineString() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[1, 2], [3, 4]]}}
      """, List.of(
      new GeoJsonFeature(newLineString(1, 2, 3, 4), Map.of())
    ));
  }

  @Test
  void testPolygon() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}}
      """, List.of(
      new GeoJsonFeature(newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), Map.of())
    ));
  }

  @Test
  void testPolygonWithHole() {
    testParse(
      """
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [3, 0], [3, 3], [0, 3], [0, 0]], [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]]}}
        """,
      List.of(
        new GeoJsonFeature(newPolygon(
          newCoordinateList(0, 0, 3, 0, 3, 3, 0, 3, 0, 0),
          List.of(newCoordinateList(1, 1, 2, 1, 2, 2, 1, 2, 1, 1))
        ), Map.of())
      ));
  }

  @Test
  void testMultiPoint() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[1, 2], [3, 4]]}}
      """, List.of(
      new GeoJsonFeature(newMultiPoint(newPoint(1, 2), newPoint(3, 4)), Map.of())
    ));
  }

  @Test
  void testMultiLineString() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]}}
      """, List.of(
      new GeoJsonFeature(newMultiLineString(newLineString(1, 2, 3, 4), newLineString(5, 6, 7, 8)), Map.of())
    ));
  }

  @Test
  void testMultiPolygon() {
    testParse(
      """
        {"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]], [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]]]}}
        """,
      List.of(
        new GeoJsonFeature(
          newMultiPolygon(newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), newPolygon(0, 0, 2, 0, 2, 2, 0, 2, 0, 0)), Map.of())
      ));
  }

  @Test
  void testMultiPolygonWithHoles() {
    testParse(
      """
        {"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": [[[[0, 0], [3, 0], [3, 3], [0, 3], [0, 0]], [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]], [[[0, 0], [3, 0], [3, 3], [0, 3], [0, 0]], [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]]]}}
        """,
      List.of(
        new GeoJsonFeature(
          newMultiPolygon(
            newPolygon(
              newCoordinateList(0, 0, 3, 0, 3, 3, 0, 3, 0, 0),
              List.of(newCoordinateList(1, 1, 2, 1, 2, 2, 1, 2, 1, 1))
            ),
            newPolygon(
              newCoordinateList(0, 0, 3, 0, 3, 3, 0, 3, 0, 0),
              List.of(newCoordinateList(1, 1, 2, 1, 2, 2, 1, 2, 1, 1))
            )
          ), Map.of())
      ));
  }


  @Test
  void testEmptyPoint() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": []}}
      """, List.of(
      new GeoJsonFeature(GeoUtils.JTS_FACTORY.createPoint(), Map.of())
    ));
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "[[]]"})
  void testEmptyLineString(String coords) {
    testParse("""
      {"type": "Feature", "geometry": {"type": "LineString", "coordinates": %s}}
      """.formatted(coords), List.of(
      new GeoJsonFeature(newLineString(), Map.of())
    ));
  }


  @ParameterizedTest
  @ValueSource(strings = {"[]", "[[]]", "[[[]]]"})
  void testEmptyPolygon(String coords) {
    testParse("""
      {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": %s}}
      """.formatted(coords), List.of(
      new GeoJsonFeature(GeoUtils.JTS_FACTORY.createPolygon(), Map.of())
    ));
  }

  @Test
  void testEmptyMultiLineString() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "LineString", "coordinates": []}}
      """, List.of(
      new GeoJsonFeature(newLineString(), Map.of())
    ));
  }

  @Test
  void testPointWithNullProperties() {
    testParse(
      """
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [1, 2]},
          "properties": null
        }
        """,
      List.of(
        new GeoJsonFeature(newPoint(1, 2), Map.of())
      ));
  }

  @Test
  void testPointWithProperties() {
    testParse(
      """
        {
          "type": "Feature",
          "geometry": {"type": "Point", "coordinates": [1, 2]},
          "properties": {
            "string":  "val",
            "int": 1,
            "double": 1.2,
            "bool": true,
            "obj": {"nested": "value"},
            "list": [1,2,3],
            "geometry": [1,2]
          }
        }
        """,
      List.of(
        new GeoJsonFeature(newPoint(1, 2), Map.of(
          "string", "val",
          "int", 1,
          "double", 1.2,
          "bool", true,
          "obj", Map.of("nested", "value"),
          "list", List.of(1, 2, 3),
          "geometry", List.of(1, 2)
        ))
      ));
  }

  @Test
  void testNewlineDelimited() {
    testParse("""
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]}}
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 4]}}
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": [5, 6]}}
      """, List.of(
      new GeoJsonFeature(newPoint(1, 2), Map.of()),
      new GeoJsonFeature(newPoint(3, 4), Map.of()),
      new GeoJsonFeature(newPoint(5, 6), Map.of())
    ));
  }

  @Test
  void testFeatureCollection() {
    testParse("""
      {
        "type": "FeatureCollection",
        "features": [
          {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]}},
          {"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 4]}},
          {"type": "Feature", "geometry": {"type": "Point", "coordinates": [5, 6]}}
        ]
      }
      """, List.of(
      new GeoJsonFeature(newPoint(1, 2), Map.of()),
      new GeoJsonFeature(newPoint(3, 4), Map.of()),
      new GeoJsonFeature(newPoint(5, 6), Map.of())
    ));
  }

  @Test
  void testParseBadFeatures() {
    testParse("""
      {"type": "Garbage", "geometry": {"type": "Point", "coordinates": [1, 2]}}
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 4], "other2": "value"}, "other": "value"}
      {"type": "Feature", "geometry": {"type": "Garbage", "coordinates": [5, 6]}}
      """, List.of(
      new GeoJsonFeature(newPoint(1, 2), Map.of()),
      new GeoJsonFeature(newPoint(3, 4), Map.of())
    ), 3);
  }

  @ParameterizedTest
  @CsvSource(
    textBlock = """
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [[1, 2]]}}; POINT EMPTY
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [[1]]}}; POINT EMPTY
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": {}}}; GEOMETRYCOLLECTION EMPTY
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, {}]}}; POINT EMPTY
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [1, 2]}}; MULTIPOINT EMPTY
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[1, 2], {}]}}; MULTIPOINT ((1 2))
        {"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [{}, [1, 2]]}}; MULTIPOINT ((1 2))
        {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [1, 2]}}; LINESTRING EMPTY
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [1, 2]}}; POLYGON EMPTY
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[1, 2]]]}}; POLYGON EMPTY
        {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]}}; POLYGON EMPTY
      """,
    delimiter = ';')
  void testBadGeometries(String json, String expected) throws ParseException {
    Geometry geometry = new WKTReader().read(expected);
    testParse(json, List.of(new GeoJsonFeature(geometry, Map.of())));
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      {}
      []
      "string"
      1
      null
      false
    """, delimiter = '\t')
  void testReallyBadGeometries(String json) {
    testParse(json, List.of());
  }

  private void testParse(String json, List<GeoJsonFeature> expected) {
    testParse(json, expected, expected.size());
  }

  private void testParse(String json, List<GeoJsonFeature> expected, int numExpected) {
    GeoJson wrapper = GeoJson.from(json);
    assertEquals(expected, wrapper.stream().toList());
    assertEquals(numExpected, wrapper.count());
  }
}
