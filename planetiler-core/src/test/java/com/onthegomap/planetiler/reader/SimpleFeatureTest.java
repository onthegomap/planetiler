package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;

class SimpleFeatureTest {

  @Test
  void testFixesMultipolygonOrdering() {
    List<Coordinate> outerPoints1 = worldRectangle(0.1, 0.9);
    List<Coordinate> innerPoints1 = worldRectangle(0.2, 0.8);
    List<Coordinate> outerPoints2 = worldRectangle(0.3, 0.7);
    List<Coordinate> innerPoints2 = worldRectangle(0.4, 0.6);

    MultiPolygon multiPolygon = (MultiPolygon) SimpleFeature.fromLatLonGeometry(newMultiPolygon(
      newPolygon(outerPoints2, List.of(innerPoints2)),
      newPolygon(outerPoints1, List.of(innerPoints1))
    )).worldGeometry();

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

  @Test
  void testFromWorldGeom() {
    var worldGeom = newLineString(
      0.5, 0.5,
      0.6, 0.7
    );
    var feature = SimpleFeature.fromWorldGeometry(worldGeom);
    assertSameNormalizedFeature(worldGeom, feature.worldGeometry());
    assertSameNormalizedFeature(worldGeom, TestUtils.round(GeoUtils.latLonToWorldCoords(feature.latLonGeometry())));
  }

  static List<Geometry> lines() {
    var line = newLineString(0, 0, 1, 1);
    var lines = List.of(line);
    return List.of(
      line,
      GeoUtils.JTS_FACTORY.createGeometryCollection(lines.toArray(new Geometry[0]))
    );
  }

  @ParameterizedTest
  @MethodSource("lines")
  void testIsLine(Geometry line) {
    var world = SimpleFeature.fromWorldGeometry(line);
    var latLon = SimpleFeature.fromLatLonGeometry(line);
    assertTrue(world.canBeLine());
    assertTrue(latLon.canBeLine());

    assertFalse(world.canBePolygon());
    assertFalse(latLon.canBePolygon());

    assertFalse(world.isPoint());
    assertFalse(latLon.isPoint());
  }

  static List<Geometry> polygons() {
    var polygon = newPolygon(0, 0, 1, 1, 1, 0, 0, 0);
    var polygons = List.of(polygon);
    return List.of(
      polygon,
      GeoUtils.JTS_FACTORY.createGeometryCollection(polygons.toArray(new Geometry[0]))
    );
  }

  @ParameterizedTest
  @MethodSource("polygons")
  void testIsPolygon(Geometry polygon) {
    var world = SimpleFeature.fromWorldGeometry(polygon);
    var latLon = SimpleFeature.fromLatLonGeometry(polygon);
    assertFalse(world.canBeLine());
    assertFalse(latLon.canBeLine());

    assertTrue(world.canBePolygon());
    assertTrue(latLon.canBePolygon());

    assertFalse(world.isPoint());
    assertFalse(latLon.isPoint());
  }

  static List<Geometry> points() {
    var point = newPoint(0, 0);
    var points = List.of(point);
    return List.of(
      point,
      GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]))
    );
  }

  @ParameterizedTest
  @MethodSource("points")
  void testIsPoint(Geometry point) {
    var world = SimpleFeature.fromWorldGeometry(point);
    var latLon = SimpleFeature.fromLatLonGeometry(point);
    assertFalse(world.canBeLine());
    assertFalse(latLon.canBeLine());

    assertFalse(world.canBePolygon());
    assertFalse(latLon.canBePolygon());

    assertTrue(world.isPoint());
    assertTrue(latLon.isPoint());
  }

  static List<Geometry> mixedCollection() {
    var point = newPoint(0, 0);
    var line = newLineString(0, 0, 1, 1);
    var polygon = newPolygon(0, 0, 1, 1, 1, 0, 0, 0);
    var pl = List.of(point, line);
    var lp = List.of(line, polygon);
    var plp = List.of(point, line, polygon);
    return List.of(
      GeoUtils.JTS_FACTORY.createGeometryCollection(pl.toArray(new Geometry[0])),
      GeoUtils.JTS_FACTORY.createGeometryCollection(lp.toArray(new Geometry[0])),
      GeoUtils.JTS_FACTORY.createGeometryCollection(plp.toArray(new Geometry[0]))
    );
  }

  @ParameterizedTest
  @MethodSource("mixedCollection")
  void testMixedCollections(Geometry point) {
    var world = SimpleFeature.fromWorldGeometry(point);
    var latLon = SimpleFeature.fromLatLonGeometry(point);
    assertFalse(world.canBeLine());
    assertFalse(latLon.canBeLine());

    assertFalse(world.canBePolygon());
    assertFalse(latLon.canBePolygon());

    assertFalse(world.isPoint());
    assertFalse(latLon.isPoint());
  }

  static Stream<Arguments> collections() {
    var point1 = newPoint(0, 0);
    var point2 = newPoint(1, 1);
    var points1 = List.of(point1);
    var points2 = List.of(point1, point2);

    var line1 = newLineString(0, 0, 1, 1);
    var line2 = newLineString(1, 1, 0, 0);
    var lines1 = List.of(line1);
    var lines2 = List.of(line1, line2);

    var polygon1 = newPolygon(0, 0, 0, 1, 1, 1, 1, 0, 0, 0);
    var polygon2 = newPolygon(0.25, 0.25, 0.25, 0.75, 0.75, 0.75, 0.75, 0.25, 0.25, 0.25);
    var polygons1 = List.of(polygon1);
    var polygons2 = List.of(polygon1, polygon2);

    return Stream.of(
      Arguments.of(GeoUtils.JTS_FACTORY.createGeometryCollection(points1.toArray(new Geometry[0])), 0, 0),
      Arguments.of(GeoUtils.JTS_FACTORY.createGeometryCollection(points2.toArray(new Geometry[0])), 0.5, 0.5),
      Arguments.of(GeoUtils.JTS_FACTORY.createGeometryCollection(lines1.toArray(new Geometry[0])), 0.5, 0.5),
      Arguments.of(GeoUtils.JTS_FACTORY.createGeometryCollection(lines2.toArray(new Geometry[0])), 0.5, 0.5),
      Arguments.of(GeoUtils.JTS_FACTORY.createGeometryCollection(polygons1.toArray(new Geometry[0])), 0.5, 0.5),
      Arguments.of(GeoUtils.JTS_FACTORY.createGeometryCollection(polygons2.toArray(new Geometry[0])), 0.5, 0.5)
    );
  }

  @ParameterizedTest
  @MethodSource("collections")
  void testCollections(Geometry gc, double expectedX, double expectedY) {
    var centroid = gc.getCentroid();
    assertEquals(expectedX, centroid.getX(), 1e-5);
    assertEquals(expectedY, centroid.getY(), 1e-5);
  }
}
