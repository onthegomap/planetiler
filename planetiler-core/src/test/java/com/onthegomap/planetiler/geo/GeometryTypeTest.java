package com.onthegomap.planetiler.geo;

import static java.util.Collections.emptyList;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    Assertions.assertTrue(GeometryType.LINE.featureTest().evaluate(line));
    Assertions.assertFalse(GeometryType.LINE.featureTest().evaluate(point));
    Assertions.assertFalse(GeometryType.LINE.featureTest().evaluate(poly));

    Assertions.assertFalse(GeometryType.POINT.featureTest().evaluate(line));
    Assertions.assertTrue(GeometryType.POINT.featureTest().evaluate(point));
    Assertions.assertFalse(GeometryType.POINT.featureTest().evaluate(poly));

    Assertions.assertFalse(GeometryType.POLYGON.featureTest().evaluate(line));
    Assertions.assertFalse(GeometryType.POLYGON.featureTest().evaluate(point));
    Assertions.assertTrue(GeometryType.POLYGON.featureTest().evaluate(poly));

    Assertions.assertThrows(Exception.class, () -> GeometryType.UNKNOWN.featureTest().evaluate(point));
    Assertions.assertThrows(Exception.class, () -> GeometryType.UNKNOWN.featureTest().evaluate(line));
    Assertions.assertThrows(Exception.class, () -> GeometryType.UNKNOWN.featureTest().evaluate(poly));
  }
}
