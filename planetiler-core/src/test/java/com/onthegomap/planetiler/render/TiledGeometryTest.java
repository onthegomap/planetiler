package com.onthegomap.planetiler.render;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.util.AffineTransformation;

class TiledGeometryTest {
  private static final int Z14_TILES = 1 << 14;

  @Test
  void testPoint() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newPoint(0.5, 0.5), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertTrue(tiledGeom.test(0, 0));
    assertFalse(tiledGeom.test(0, 1));
    assertFalse(tiledGeom.test(1, 0));
    assertFalse(tiledGeom.test(1, 1));
    assertEquals(Set.of(TileCoord.ofXYZ(0, 0, 14)), tiledGeom.stream().collect(Collectors.toSet()));

    tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newPoint(Z14_TILES - 0.5, Z14_TILES - 0.5), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertTrue(tiledGeom.test(Z14_TILES - 1, Z14_TILES - 1));
    assertFalse(tiledGeom.test(Z14_TILES - 2, Z14_TILES - 1));
    assertFalse(tiledGeom.test(Z14_TILES - 1, Z14_TILES - 2));
    assertFalse(tiledGeom.test(Z14_TILES - 2, Z14_TILES - 2));
    assertEquals(Set.of(TileCoord.ofXYZ(Z14_TILES - 1, Z14_TILES - 1, 14)),
      tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testMultiPoint() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newMultiPoint(
      TestUtils.newPoint(0.5, 0.5),
      TestUtils.newPoint(2.5, 1.5)
    ), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(
      TileCoord.ofXYZ(0, 0, 14),
      TileCoord.ofXYZ(2, 1, 14)
    ), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testLine() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newLineString(
      0.5, 0.5,
      1.5, 0.5
    ), 14, new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(
      TileCoord.ofXYZ(0, 0, 14),
      TileCoord.ofXYZ(1, 0, 14)
    ), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testMultiLine() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newMultiLineString(
      TestUtils.newLineString(
        0.5, 0.5,
        1.5, 0.5
      ),
      TestUtils.newLineString(
        3.5, 1.5,
        4.5, 1.5
      )
    ), 14, new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(
      TileCoord.ofXYZ(0, 0, 14),
      TileCoord.ofXYZ(1, 0, 14),
      TileCoord.ofXYZ(3, 1, 14),
      TileCoord.ofXYZ(4, 1, 14)
    ), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testPolygon() throws GeometryException {
    var tiledGeom =
      TiledGeometry.getCoveredTiles(TestUtils.newPolygon(
        TestUtils.rectangleCoordList(25.5, 27.5),
        List.of(TestUtils.rectangleCoordList(25.9, 27.1))
      ), 14, new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(
      TileCoord.ofXYZ(25, 25, 14),
      TileCoord.ofXYZ(26, 25, 14),
      TileCoord.ofXYZ(27, 25, 14),

      TileCoord.ofXYZ(25, 26, 14),
      //      TileCoord.ofXYZ(26, 26, 14), skipped because of hole!
      TileCoord.ofXYZ(27, 26, 14),

      TileCoord.ofXYZ(25, 27, 14),
      TileCoord.ofXYZ(26, 27, 14),
      TileCoord.ofXYZ(27, 27, 14)
    ), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testMultiPolygon() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newMultiPolygon(
      TestUtils.rectangle(25.5, 26.5),
      TestUtils.rectangle(30.1, 30.9)
    ), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(
      TileCoord.ofXYZ(25, 25, 14),
      TileCoord.ofXYZ(25, 26, 14),
      TileCoord.ofXYZ(26, 25, 14),
      TileCoord.ofXYZ(26, 26, 14),

      TileCoord.ofXYZ(30, 30, 14)
    ), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testEmpty() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newGeometryCollection(), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testGeometryCollection() throws GeometryException {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newGeometryCollection(
      TestUtils.rectangle(0.1, 0.9),
      TestUtils.newPoint(1.5, 1.5),
      TestUtils.newGeometryCollection(TestUtils.newLineString(3.5, 10.5, 4.5, 10.5))
    ), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(
      // rectangle
      TileCoord.ofXYZ(0, 0, 14),

      // point
      TileCoord.ofXYZ(1, 1, 14),

      // linestring
      TileCoord.ofXYZ(3, 10, 14),
      TileCoord.ofXYZ(4, 10, 14)
    ), tiledGeom.stream().collect(Collectors.toSet()));
  }

  private void rotate(CoordinateSequence coordinateSequence, double x, double y, int degrees) {
    var transformation = AffineTransformation.rotationInstance(Math.toRadians(degrees), x, y);
    for (int i = 0; i < coordinateSequence.size(); i++) {
      transformation.transform(coordinateSequence, i);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 90, 180, 270})
  void testOnlyHoleTouchesOtherCellBottom(int degrees) {
    // hole falls outside shell, and hole touches neighboring tile
    // make sure that we don't emit that other tile at all
    MutableCoordinateSequence outer = new MutableCoordinateSequence();
    outer.addPoint(1.5, 1.5);
    outer.addPoint(1.6, 1.5);
    outer.addPoint(1.5, 1.6);
    outer.closeRing();
    MutableCoordinateSequence inner = new MutableCoordinateSequence();
    inner.addPoint(1.4, 1.8);
    inner.addPoint(1.6, 1.8);
    inner.addPoint(1.5, 2);
    inner.closeRing();
    rotate(outer, 1.5, 1.5, degrees);
    rotate(inner, 1.5, 1.5, degrees);

    List<List<CoordinateSequence>> coordinateSequences = List.of(List.of(outer, inner));
    var extent = new TileExtents.ForZoom(11, 0, 0, 1 << 11, 1 << 11, null);
    assertThrows(GeometryException.class,
      () -> TiledGeometry.sliceIntoTiles(coordinateSequences, 0.1, true, 11, extent));
  }
}
