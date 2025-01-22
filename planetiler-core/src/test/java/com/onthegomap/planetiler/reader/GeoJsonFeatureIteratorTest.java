package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GeoJsonFeatureIteratorTest {
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
      {"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 4]}}
      {"type": "Feature", "geometry": {"type": "Garbage", "coordinates": [5, 6]}}
      """, List.of(
      new GeoJsonFeature(newPoint(1, 2), Map.of()),
      new GeoJsonFeature(newPoint(3, 4), Map.of())
    ), 3);
  }

  private void testParse(String json, List<GeoJsonFeature> expected) {
    testParse(json, expected, expected.size());
  }

  private void testParse(String json, List<GeoJsonFeature> expected, int numExpected) {
    GeoJson wrapper = new GeoJson(() -> new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    assertEquals(expected, wrapper.stream().toList());
    assertEquals(numExpected, wrapper.count());
  }
}
