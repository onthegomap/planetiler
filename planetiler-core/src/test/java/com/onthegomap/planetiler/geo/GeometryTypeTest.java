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

    var sf =
      SimpleFeature.createFakeOsmFeature(TestUtils.newLineString(0, 0, 1, 0, 1, 1), tags, "osm", null, 1, emptyList());

    Assertions.assertTrue(GeometryType.LINE.featureTest().test(sf));
    Assertions.assertFalse(GeometryType.POINT.featureTest().test(sf));
    Assertions.assertFalse(GeometryType.POLYGON.featureTest().test(sf));

    sf =
      SimpleFeature.createFakeOsmFeature(TestUtils.newPoint(0, 0), tags, "osm", null, 1, emptyList());

    Assertions.assertFalse(GeometryType.LINE.featureTest().test(sf));
    Assertions.assertTrue(GeometryType.POINT.featureTest().test(sf));
    Assertions.assertFalse(GeometryType.POLYGON.featureTest().test(sf));

    sf =
      SimpleFeature.createFakeOsmFeature(TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 0), tags, "osm", null, 1,
        emptyList());

    Assertions.assertFalse(GeometryType.LINE.featureTest().test(sf));
    Assertions.assertFalse(GeometryType.POINT.featureTest().test(sf));
    Assertions.assertTrue(GeometryType.POLYGON.featureTest().test(sf));


  }
}
