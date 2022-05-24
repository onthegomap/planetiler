package com.onthegomap.planetiler.geo;

import static java.util.Collections.emptyList;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GeometryTypeTest {

  @Test
  void testGeometryFactory() throws Exception {
    Map<String, Object> tags = Map.of("key1", "value1");

    var line =
      SimpleFeature.createFakeOsmFeature(TestUtils.newLineString(0, 0, 1, 0, 1, 1), tags, "osm", null, 1, emptyList());
    var point =
      SimpleFeature.createFakeOsmFeature(TestUtils.newPoint(0, 0), tags, "osm", null, 1, emptyList());
    var poly =
      SimpleFeature.createFakeOsmFeature(TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 0), tags, "osm", null, 1,
        emptyList());

    Assertions.assertTrue(GeometryType.LINE.featureTest().test(line));
    Assertions.assertFalse(GeometryType.LINE.featureTest().test(point));
    Assertions.assertFalse(GeometryType.LINE.featureTest().test(poly));

    Assertions.assertFalse(GeometryType.POINT.featureTest().test(line));
    Assertions.assertTrue(GeometryType.POINT.featureTest().test(point));
    Assertions.assertFalse(GeometryType.POINT.featureTest().test(poly));

    Assertions.assertFalse(GeometryType.POLYGON.featureTest().test(line));
    Assertions.assertFalse(GeometryType.POLYGON.featureTest().test(point));
    Assertions.assertTrue(GeometryType.POLYGON.featureTest().test(poly));

    Assertions.assertThrows(Exception.class, () -> GeometryType.UNKNOWN.featureTest().test(point));
    Assertions.assertThrows(Exception.class, () -> GeometryType.UNKNOWN.featureTest().test(line));
    Assertions.assertThrows(Exception.class, () -> GeometryType.UNKNOWN.featureTest().test(poly));
  }
}
