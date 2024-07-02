package com.onthegomap.planetiler.geo;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

class GeometryTypeTest {

  @Test
  void testGeometryFactory() {
    Map<String, Object> tags = Map.of("key1", "value1");

    var line =
      SimpleFeature.createFakeOsmFeature(TestUtils.newLineString(0, 0, 1, 0, 1, 1), tags, "osm", null, 1, emptyList());
    var point =
      SimpleFeature.createFakeOsmFeature(TestUtils.newPoint(0, 0), tags, "osm", null, 1, emptyList());
    var poly =
      SimpleFeature.createFakeOsmFeature(TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 0), tags, "osm", null, 1,
        emptyList());

    assertTrue(GeometryType.LINE.featureTest().evaluate(line));
    assertFalse(GeometryType.LINE.featureTest().evaluate(point));
    assertFalse(GeometryType.LINE.featureTest().evaluate(poly));

    assertFalse(GeometryType.POINT.featureTest().evaluate(line));
    assertTrue(GeometryType.POINT.featureTest().evaluate(point));
    assertFalse(GeometryType.POINT.featureTest().evaluate(poly));

    assertFalse(GeometryType.POLYGON.featureTest().evaluate(line));
    assertFalse(GeometryType.POLYGON.featureTest().evaluate(point));
    assertTrue(GeometryType.POLYGON.featureTest().evaluate(poly));

    assertTrue(GeometryType.UNKNOWN.featureTest().evaluate(point));
    assertTrue(GeometryType.UNKNOWN.featureTest().evaluate(line));
    assertTrue(GeometryType.UNKNOWN.featureTest().evaluate(poly));
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT; POINT EMPTY",
    "POINT; POINT(1 1)",
    "POINT; MULTIPOINT(1 1, 2 2)",
    "LINE; lineString(1 1, 2 2)",
    "LINE; LINESTRING ZM(1 1 2 3, 2 2 4 5)",
    "LINE; multiLineString((1 1, 2 2))",
    "POLYGON; POLYGON((0 0, 0 1, 1 0, 0 0))",
    "POLYGON; MULTIPOLYGON(((0 0, 0 1, 1 0, 0 0)))",
    "UNKNOWN; GEOMETRYCOLLECTION EMPTY",
  }, delimiter = ';')
  void testSniffTypes(GeometryType expected, String wkt) throws ParseException {
    assertEquals(expected, GeometryType.fromWKT(wkt));
    var wkb = new WKBWriter().write(new WKTReader().read(wkt));
    assertEquals(expected, GeometryType.fromWKB(wkb));
  }
}
