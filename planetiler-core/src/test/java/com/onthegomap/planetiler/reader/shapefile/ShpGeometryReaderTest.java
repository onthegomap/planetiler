package com.onthegomap.planetiler.reader.shapefile;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.densify.Densifier;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

class ShpGeometryReaderTest {

  private static List<Coordinate> shell(double minX, double minY, double maxX, double maxY) {
    return rectangleCoordList(minX, minY, maxX, maxY).reversed();
  }

  private static List<Coordinate> hole(double minX, double minY, double maxX, double maxY) {
    return rectangleCoordList(minX, minY, maxX, maxY);
  }

  private static List<Coordinate> denseHole(double minX, double minY, double maxX, double maxY, int pointsPerSide) {
    Polygon dense = (Polygon) Densifier.densify(rectangle(minX, minY, maxX, maxY), (maxX - minX) / pointsPerSide);
    return List.of(dense.getExteriorRing().getCoordinates());
  }

  private static List<Coordinate> denseShell(double minX, double minY, double maxX, double maxY, int pointsPerSide) {
    return denseHole(minX, minY, maxX, maxY, pointsPerSide).reversed();
  }

  private static byte[] polygonRecord(List<List<Coordinate>> rings) {
    var parts = rings.stream()
      .map(ring -> JTS_FACTORY.getCoordinateSequenceFactory().create(ring.toArray(Coordinate[]::new)))
      .toList();
    return TestShapefileWriter.partsRecord(ShapeType.POLYGON, parts);
  }

  private static Geometry read(List<List<Coordinate>> rings) {
    return ShpGeometryReader.read(polygonRecord(rings));
  }

  private static void assertReads(Geometry expected, List<List<Coordinate>> rings) {
    Geometry actual = read(rings);
    assertTrue(expected.equalsExact(actual), () -> "expected " + expected + " got " + actual);
  }

  @Test
  void testOneOuterRingWithHoles() {
    var rings = List.of(
      shell(0, 0, 10, 10),
      hole(1, 1, 3, 3),
      hole(5, 5, 7, 7)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10), List.of(hole(1, 1, 3, 3), hole(5, 5, 7, 7)))
    ), rings);
  }

  @Test
  void testHolesSplitBetweenOuterRings() {
    var rings = List.of(
      hole(5, 5, 7, 7),
      shell(0, 0, 10, 10),
      hole(25, 5, 27, 7),
      shell(20, 0, 30, 10),
      hole(1, 1, 3, 3),
      hole(21, 1, 23, 3)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10), List.of(hole(5, 5, 7, 7), hole(1, 1, 3, 3))),
      newPolygon(shell(20, 0, 30, 10), List.of(hole(25, 5, 27, 7), hole(21, 1, 23, 3)))
    ), rings);
  }

  @Test
  void testHoleGoesToSmallestContainingOuterRing() {
    // island with a pond in a lake in an island, in mixed record order
    var rings = List.of(
      hole(4, 4, 5, 5),
      shell(0, 0, 20, 20),
      hole(2, 2, 12, 12),
      shell(3, 3, 8, 8)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 20, 20), List.of(hole(2, 2, 12, 12))),
      newPolygon(shell(3, 3, 8, 8), List.of(hole(4, 4, 5, 5)))
    ), rings);
  }

  @Test
  void testOrphanHoleWithOtherOuterRings() {
    var rings = List.of(
      shell(0, 0, 10, 10),
      hole(20, 20, 25, 25),
      shell(30, 0, 40, 10)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10)),
      newPolygon(shell(30, 0, 40, 10)),
      newPolygon(hole(20, 20, 25, 25))
    ), rings);
  }

  @Test
  void testEveryHoleGoesIntoOnlyOuterRing() {
    // a hole outside the only outer ring makes the polygon invalid, as the record describes
    var outer = newCoordinateList(0, 0, 0, 10, 5, 10, 5, 5, 10, 5, 10, 0, 0, 0);
    var rings = List.of(outer, hole(6, 6, 8, 8), hole(20, 20, 21, 21));
    assertReads(newMultiPolygon(newPolygon(outer, List.of(hole(6, 6, 8, 8), hole(20, 20, 21, 21)))), rings);
  }

  @Test
  void testOnlyHoles() {
    // a later hole goes into an earlier orphan hole that contains it
    var rings = List.of(
      hole(0, 0, 10, 10),
      hole(20, 0, 21, 1),
      hole(2, 2, 4, 4)
    );
    assertReads(newMultiPolygon(
      newPolygon(hole(0, 0, 10, 10), List.of(hole(2, 2, 4, 4))),
      newPolygon(hole(20, 0, 21, 1))
    ), rings);
  }

  @Test
  void testHoleTouchingSmallOuterRingVertex() {
    var triangle = newCoordinateList(0, 0, 5, 2, 2, 5, 0, 0);
    var rings = List.of(
      shell(-10, -10, 0, 0),
      shell(0, 0, 10, 10),
      triangle
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(-10, -10, 0, 0)),
      newPolygon(shell(0, 0, 10, 10), List.of(triangle))
    ), rings);
  }

  @Test
  void testHoleTouchingLargeOuterRingVertex() {
    var triangle = newCoordinateList(0, 0, 50, 20, 20, 50, 0, 0);
    assertReads(newMultiPolygon(
      newPolygon(denseShell(-100, -100, 0, 0, 50)),
      newPolygon(denseShell(0, 0, 100, 100, 50), List.of(triangle))
    ), List.of(denseShell(-100, -100, 0, 0, 50), denseShell(0, 0, 100, 100, 50), triangle));
    assertReads(newMultiPolygon(
      newPolygon(denseShell(0, 0, 100, 100, 50), List.of(triangle, hole(10, 60, 15, 65)))
    ), List.of(denseShell(0, 0, 100, 100, 50), triangle, hole(10, 60, 15, 65)));
  }

  @Test
  void testHoleWithAllPointsOnOuterRing() {
    // a hole that is the outer ring reversed has no vertex off the outer ring, but its interior is inside it
    var rings = List.of(
      denseShell(0, 0, 100, 100, 50),
      denseHole(0, 0, 100, 100, 50)
    );
    Polygon polygon = (Polygon) read(rings).getGeometryN(0);
    assertEquals(1, polygon.getNumInteriorRing());
  }

  @Test
  void testDuplicateOuterRings() {
    var rings = List.of(
      shell(0, 0, 10, 10),
      shell(0, 0, 10, 10),
      hole(1, 1, 3, 3)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10), List.of(hole(1, 1, 3, 3))),
      newPolygon(shell(0, 0, 10, 10))
    ), rings);
  }

  @Test
  void testManyHolesInLargeOuterRings() {
    List<List<Coordinate>> rings = new ArrayList<>(List.of(
      denseShell(0, 0, 100, 100, 200),
      denseShell(200, 0, 300, 100, 200)
    ));
    for (int x = 0; x < 20; x++) {
      for (int y = 0; y < 20; y++) {
        rings.add(hole(1 + x * 5, 1 + y * 5, 3 + x * 5, 3 + y * 5));
        rings.add(hole(201 + x * 5, 1 + y * 5, 203 + x * 5, 3 + y * 5));
      }
    }
    Geometry result = read(rings);
    assertEquals(2, result.getNumGeometries());
    assertEquals(400, ((Polygon) result.getGeometryN(0)).getNumInteriorRing());
    assertEquals(400, ((Polygon) result.getGeometryN(1)).getNumInteriorRing());
  }

  @Test
  void testClosesUnclosedRings() {
    var rings = List.of(
      newCoordinateList(0, 0, 0, 10, 10, 10, 10, 0),
      newCoordinateList(1, 1, 3, 1, 3, 3, 1, 3)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10), List.of(hole(1, 1, 3, 3)))
    ), rings);
  }

  @Test
  void testSkipsShortAndEmptyRings() {
    var rings = List.of(
      List.<Coordinate>of(),
      shell(0, 0, 10, 10),
      newCoordinateList(1, 1, 2, 2, 1, 1),
      List.<Coordinate>of(),
      newCoordinateList(5, 5, 6, 6)
    );
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10))
    ), rings);
  }

  @Test
  void testRecordWithoutUsableRingThrows() {
    byte[] onlyShortRings = polygonRecord(List.of(
      List.of(),
      newCoordinateList(1, 1, 2, 2, 1, 1)
    ));
    assertThrows(IllegalArgumentException.class, () -> ShpGeometryReader.read(onlyShortRings));
    byte[] noRings = polygonRecord(List.of());
    assertThrows(IllegalArgumentException.class, () -> ShpGeometryReader.read(noRings));
  }

  @Test
  void testNullRecordThrows() {
    byte[] nullRecord = new byte[4];
    assertThrows(IllegalArgumentException.class, () -> ShpGeometryReader.read(nullRecord));
  }

  @Test
  void testReturnsMultiPolygonForOneRing() {
    Geometry result = read(List.of(shell(0, 0, 10, 10)));
    assertEquals("MultiPolygon", result.getGeometryType());
    LinearRing shell = ((Polygon) result.getGeometryN(0)).getExteriorRing();
    assertEquals(new Coordinate(0, 10), shell.getCoordinateN(1));
  }

  /** A clockwise comb: a body from y=0 to 10 with {@code teeth} teeth from x=2i to 2i+1 reaching up to y=20. */
  private static List<Coordinate> comb(int teeth) {
    List<Coordinate> points = new ArrayList<>(newCoordinateList(0, 0, 0, 10));
    for (int i = 0; i < teeth; i++) {
      points.addAll(newCoordinateList(2d * i, 20, 2d * i + 1, 20, 2d * i + 1, 10, 2d * i + 2, 10));
    }
    points.addAll(newCoordinateList(2d * teeth, 0, 0, 0));
    return points;
  }

  @Test
  void testHolesInAndBetweenTeethOfLargeNonConvexOuterRing() {
    List<List<Coordinate>> rings = new ArrayList<>(List.of(
      comb(50),
      shell(1000, 0, 1010, 10)
    ));
    List<List<Coordinate>> toothHoles = new ArrayList<>();
    List<Polygon> gapHoles = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      // inside a tooth, then in the gap after it (outside the comb, but inside its envelope)
      var toothHole = hole(2 * i + 0.25, 12 + i % 5, 2 * i + 0.75, 12.5 + i % 5);
      var gapHole = hole(2 * i + 1.25, 15, 2 * i + 1.75, 15.5);
      rings.add(toothHole);
      rings.add(gapHole);
      toothHoles.add(toothHole);
      gapHoles.add(newPolygon(gapHole));
    }
    // a hole outside every outer ring becomes a polygon of its own, after the outer rings
    List<Polygon> expected =
      new ArrayList<>(List.of(newPolygon(comb(50), toothHoles), newPolygon(shell(1000, 0, 1010, 10))));
    expected.addAll(gapHoles);
    assertReads(newMultiPolygon(expected.toArray(Polygon[]::new)), rings);
  }

  @Test
  void testHoleLevelWithOuterRingVertices() {
    // a ray from each hole's first point (y=10) passes through every vertex where the teeth meet the body
    List<List<Coordinate>> rings = new ArrayList<>(List.of(
      comb(50),
      shell(1000, 0, 1010, 10)
    ));
    List<List<Coordinate>> holes = new ArrayList<>();
    for (int i = 0; i < 50; i += 7) {
      holes.add(hole(2 * i + 0.25, 10, 2 * i + 0.75, 10.5));
    }
    rings.addAll(holes);
    assertReads(newMultiPolygon(newPolygon(comb(50), holes), newPolygon(shell(1000, 0, 1010, 10))), rings);
  }

  @Test
  void testFlatOuterRing() {
    var flat = newCoordinateList(0, 0, 5, 0, 10, 0, 0, 0);
    assertReads(newMultiPolygon(
      newPolygon(flat),
      newPolygon(shell(20, 0, 30, 10), List.of(hole(21, 1, 23, 3)))
    ), List.of(
      flat,
      shell(20, 0, 30, 10),
      hole(21, 1, 23, 3)
    ));
    assertReads(newMultiPolygon(
      newPolygon(flat, List.of(hole(1, 1, 2, 2)))
    ), List.of(
      flat,
      hole(1, 1, 2, 2)
    ));
  }
}
