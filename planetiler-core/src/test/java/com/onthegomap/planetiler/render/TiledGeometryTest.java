package com.onthegomap.planetiler.render;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TiledGeometryTest {
  private static final int Z14_TILES = 1 << 14;

  @Test
  void testPoint() {
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
  void testMultiPoint() {
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
  void testLine() {
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
  void testMultiLine() {
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
  void testPolygon() {
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
  void testMultiPolygon() {
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
  void testEmpty() {
    var tiledGeom = TiledGeometry.getCoveredTiles(TestUtils.newGeometryCollection(), 14,
      new TileExtents.ForZoom(14, 0, 0, Z14_TILES, Z14_TILES, null));
    assertEquals(Set.of(), tiledGeom.stream().collect(Collectors.toSet()));
  }

  @Test
  void testGeometryCollection() {
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
}
