package com.onthegomap.flatmap.reader;

import static com.onthegomap.flatmap.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiPolygon;

public class ReaderFeatureTest {

  @Test
  public void testFixesMultipolygonOrdering() {
    List<Coordinate> outerPoints1 = worldRectangle(0.1, 0.9);
    List<Coordinate> innerPoints1 = worldRectangle(0.2, 0.8);
    List<Coordinate> outerPoints2 = worldRectangle(0.3, 0.7);
    List<Coordinate> innerPoints2 = worldRectangle(0.4, 0.6);

    MultiPolygon multiPolygon = (MultiPolygon) new ReaderFeature(newMultiPolygon(
      newPolygon(outerPoints2, List.of(innerPoints2)),
      newPolygon(outerPoints1, List.of(innerPoints1))
    ), Map.of(), 1).worldGeometry();

    assertEquals(2, multiPolygon.getNumGeometries());
    assertSameNormalizedFeature(round(newPolygon(
      rectangleCoordList(0.1, 0.9),
      List.of(rectangleCoordList(0.2, 0.8))
    )), round(multiPolygon.getGeometryN(0)));
    assertSameNormalizedFeature(round(newPolygon(
      rectangleCoordList(0.3, 0.7),
      List.of(rectangleCoordList(0.4, 0.6))
    )), round(multiPolygon.getGeometryN(1)));
  }
}
