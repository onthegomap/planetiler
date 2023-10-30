package com.onthegomap.planetiler.render;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
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

  private void flipAndRotate(CoordinateSequence coordinateSequence, double x, double y, boolean flipX, boolean flipY,
    int degrees) {
    if (flipX) {
      var transformation = AffineTransformation.reflectionInstance(x, y, x, y + 1);
      for (int i = 0; i < coordinateSequence.size(); i++) {
        transformation.transform(coordinateSequence, i);
      }
    }
    if (flipY) {
      var transformation = AffineTransformation.reflectionInstance(x, y, x + 1, y);
      for (int i = 0; i < coordinateSequence.size(); i++) {
        transformation.transform(coordinateSequence, i);
      }
    }
    rotate(coordinateSequence, x, y, degrees);
    // maintain winding order if we did a single flip
    // doing a second flip fixes winding order itself
    if (flipX ^ flipY) {
      CoordinateSequences.reverse(coordinateSequence);
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

  @ParameterizedTest
  @CsvSource({
    "0,false,false",
    "90,false,false",
    "180,false,false",
    "270,false,false",
    "0,true,false",
    "0,false,true",
    "0,true,true",
  })
  void testOverlappingHoles(int degrees, boolean flipX, boolean flipY) throws GeometryException {
    MutableCoordinateSequence outer = new MutableCoordinateSequence();
    outer.addPoint(1, 1);
    outer.addPoint(10, 1);
    outer.addPoint(10, 10);
    outer.addPoint(1, 10);
    outer.closeRing();
    MutableCoordinateSequence inner1 = new MutableCoordinateSequence();
    inner1.addPoint(2, 2);
    inner1.addPoint(2, 9);
    inner1.addPoint(9, 9);
    inner1.addPoint(3, 5);
    inner1.addPoint(9, 2);
    inner1.closeRing();
    MutableCoordinateSequence inner2 = new MutableCoordinateSequence();
    inner2.addPoint(9, 3);
    inner2.addPoint(9, 8);
    inner2.addPoint(4, 5);
    inner2.closeRing();
    flipAndRotate(outer, 6, 6, flipX, flipY, degrees);
    flipAndRotate(inner1, 6, 6, flipX, flipY, degrees);
    flipAndRotate(inner2, 6, 6, flipX, flipY, degrees);

    testRender(List.of(List.of(outer, inner1)));
    testRender(List.of(List.of(outer, inner2)));
    testRender(List.of(List.of(outer, inner1, inner2)));
    var result = testRender(List.of(List.of(outer, inner2, inner1)));
    if (degrees == 0 && !flipX && !flipY) {
      assertFalse(result.getCoveredTiles().test(7, 4));
      assertFalse(result.getCoveredTiles().test(3, 3));
      assertTrue(result.getCoveredTiles().test(1, 1));
      assertTrue(result.getCoveredTiles().test(9, 9));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "0,false,false",
    "90,false,false",
    "180,false,false",
    "270,false,false",
    "0,true,false",
    "0,false,true",
    "0,true,true",
  })
  void testInsideComplexHole(int degrees, boolean flipX, boolean flipY) throws GeometryException {
    MutableCoordinateSequence outer = new MutableCoordinateSequence();
    outer.addPoint(1, 1);
    outer.addPoint(10, 1);
    outer.addPoint(10, 10);
    outer.addPoint(1, 10);
    outer.closeRing();
    MutableCoordinateSequence inner1 = new MutableCoordinateSequence();
    inner1.addPoint(6.5, 1.5);
    inner1.addPoint(2, 2);
    inner1.addPoint(2, 9);
    inner1.addPoint(9, 9);
    inner1.addPoint(9, 2);
    inner1.addPoint(4.6, 2);
    inner1.addPoint(8, 8);
    inner1.addPoint(3, 8);
    inner1.addPoint(4, 2);
    inner1.closeRing();
    MutableCoordinateSequence inner2 = new MutableCoordinateSequence();
    inner2.addPoint(5.5, 6.5);
    inner2.addPoint(5.5, 6.6);
    inner2.addPoint(5.6, 6.6);
    inner2.closeRing();
    flipAndRotate(outer, 6, 6, flipX, flipY, degrees);
    flipAndRotate(inner1, 6, 6, flipX, flipY, degrees);
    flipAndRotate(inner2, 6, 6, flipX, flipY, degrees);

    assertTrue(Orientation.isCCW(outer));
    assertFalse(Orientation.isCCW(inner1));
    assertFalse(Orientation.isCCW(inner2));

    testRender(List.of(List.of(outer, inner1)));
    testRender(List.of(List.of(outer, inner2)));
    testRender(List.of(List.of(outer, inner1, inner2)));
    var result = testRender(List.of(List.of(outer, inner2, inner1)));
    if (degrees == 0 && !flipX && !flipY) {
      var filled = StreamSupport.stream(result.getFilledTiles().spliterator(), false).collect(Collectors.toSet());
      assertTrue(filled.contains(TileCoord.ofXYZ(5, 5, 14)));
      assertTrue(filled.contains(TileCoord.ofXYZ(4, 6, 14)));
      assertFalse(filled.contains(TileCoord.ofXYZ(5, 6, 14)));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "0,false,false",
    "90,false,false",
    "180,false,false",
    "270,false,false",
    "0,true,false",
    "0,false,true",
    "0,true,true",
  })
  void testSideOfHoleIntercepted(int degrees, boolean flipX, boolean flipY) throws GeometryException {
    MutableCoordinateSequence outer = new MutableCoordinateSequence();
    outer.addPoint(1, 1);
    outer.addPoint(10, 1);
    outer.addPoint(10, 10);
    outer.addPoint(1, 10);
    outer.closeRing();
    MutableCoordinateSequence inner1 = new MutableCoordinateSequence();
    inner1.addPoint(2, 2);
    inner1.addPoint(2, 9);
    inner1.addPoint(9, 9);
    inner1.addPoint(3, 5);
    inner1.addPoint(9, 2);
    inner1.addPoint(9, 4.2);
    inner1.addPoint(7.5, 4.2);
    inner1.addPoint(7.5, 4.8);
    inner1.addPoint(9.5, 4.8);
    inner1.addPoint(9.5, 1.8);
    inner1.closeRing();
    flipAndRotate(outer, 5, 5, flipX, flipY, degrees);
    flipAndRotate(inner1, 5, 5, flipX, flipY, degrees);

    var result = testRender(List.of(List.of(outer, inner1)));
    if (degrees == 0 && !flipX && !flipY) {
      var filled = StreamSupport.stream(result.getFilledTiles().spliterator(), false).collect(Collectors.toSet());
      assertFalse(filled.contains(TileCoord.ofXYZ(7, 4, 14)), filled.toString());
      var tileData = result.getTileData().get(TileCoord.ofXYZ(7, 4, 14));
      var normalized = tileData.stream().map(items -> items.stream().map(coordinateSequence -> {
        for (int i = 0; i < coordinateSequence.size(); i++) {
          coordinateSequence.setOrdinate(i, 0, Math.round(coordinateSequence.getX(i) * 10) / 10d);
          coordinateSequence.setOrdinate(i, 1, Math.round(coordinateSequence.getY(i) * 10) / 10d);
        }
        return List.of(coordinateSequence.toCoordinateArray());
      }).toList()).toList();
      assertEquals(
        List.of(List.of(
          List.of(GeoUtils.coordinateSequence(
            0, 0,
            256, 0,
            256, 256,
            0, 256,
            0, 0
          ).toCoordinateArray()),
          List.of(GeoUtils.coordinateSequence(
            -0, 256,
            -0, 0,
            256, 0,
            256, 51.2,
            128, 51.2,
            128, 204.8,
            256, 204.8,
            256, 0,
            0, 0,
            0, 256
          ).toCoordinateArray())
        )),
        normalized
      );
    }
  }

  private static TiledGeometry testRender(List<List<CoordinateSequence>> coordinateSequences) throws GeometryException {
    return TiledGeometry.sliceIntoTiles(
      coordinateSequences, 0, true, 14,
      new TileExtents.ForZoom(14, -10, -10, 1 << 14, 1 << 14, null)
    );
  }
}
