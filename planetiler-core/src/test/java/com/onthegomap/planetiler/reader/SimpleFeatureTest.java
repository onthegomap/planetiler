package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiPolygon;

public class SimpleFeatureTest {

  @Test
  public void testFixesMultipolygonOrdering() {
    List<Coordinate> outerPoints1 = worldRectangle(0.1, 0.9);
    List<Coordinate> innerPoints1 = worldRectangle(0.2, 0.8);
    List<Coordinate> outerPoints2 = worldRectangle(0.3, 0.7);
    List<Coordinate> innerPoints2 = worldRectangle(0.4, 0.6);

    MultiPolygon multiPolygon =
        (MultiPolygon)
            SimpleFeature.fromLatLonGeometry(
                    newMultiPolygon(
                        newPolygon(outerPoints2, List.of(innerPoints2)),
                        newPolygon(outerPoints1, List.of(innerPoints1))))
                .worldGeometry();

    assertEquals(2, multiPolygon.getNumGeometries());
    assertSameNormalizedFeature(
        round(newPolygon(rectangleCoordList(0.1, 0.9), List.of(rectangleCoordList(0.2, 0.8)))),
        round(multiPolygon.getGeometryN(0)));
    assertSameNormalizedFeature(
        round(newPolygon(rectangleCoordList(0.3, 0.7), List.of(rectangleCoordList(0.4, 0.6)))),
        round(multiPolygon.getGeometryN(1)));
  }

  @Test
  public void testFromWorldGeom() {
    var worldGeom =
        newLineString(
            0.5, 0.5,
            0.6, 0.7);
    var feature = SimpleFeature.fromWorldGeometry(worldGeom);
    assertSameNormalizedFeature(worldGeom, feature.worldGeometry());
    assertSameNormalizedFeature(
        worldGeom, TestUtils.round(GeoUtils.latLonToWorldCoords(feature.latLonGeometry())));
  }

  @Test
  public void testIsLine() {
    var world = SimpleFeature.fromWorldGeometry(newLineString(0, 0, 1, 1));
    var latLon = SimpleFeature.fromLatLonGeometry(newLineString(0, 0, 1, 1));
    assertTrue(world.canBeLine());
    assertTrue(latLon.canBeLine());

    assertFalse(world.canBePolygon());
    assertFalse(latLon.canBePolygon());

    assertFalse(world.isPoint());
    assertFalse(latLon.isPoint());
  }

  @Test
  public void testIsPolygon() {
    var world = SimpleFeature.fromWorldGeometry(newPolygon(0, 0, 1, 1, 1, 0, 0, 0));
    var latLon = SimpleFeature.fromLatLonGeometry(newPolygon(0, 0, 1, 1, 1, 0, 0, 0));
    assertFalse(world.canBeLine());
    assertFalse(latLon.canBeLine());

    assertTrue(world.canBePolygon());
    assertTrue(latLon.canBePolygon());

    assertFalse(world.isPoint());
    assertFalse(latLon.isPoint());
  }

  @Test
  public void testIsPoint() {
    var world = SimpleFeature.fromWorldGeometry(newPoint(0, 0));
    var latLon = SimpleFeature.fromLatLonGeometry(newPoint(0, 0));
    assertFalse(world.canBeLine());
    assertFalse(latLon.canBeLine());

    assertFalse(world.canBePolygon());
    assertFalse(latLon.canBePolygon());

    assertTrue(world.isPoint());
    assertTrue(latLon.isPoint());
  }
}
