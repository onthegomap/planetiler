package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.TestUtils.newCoordinateList;
import static com.onthegomap.planetiler.TestUtils.newMultiPolygon;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.TestUtils.rectangleCoordList;
import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.shapefile.shp.PolygonHandler;
import org.geotools.data.shapefile.shp.ShapeType;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.densify.Densifier;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

class ShapefilePolygonHandlerTest {

  /** Record contents per the spec's "Polygon Record Contents", positioned after the shape type like GeoTools does. */
  private static ByteBuffer polygonRecord(List<List<Coordinate>> rings) {
    int numPoints = 0;
    for (List<Coordinate> ring : rings) {
      numPoints += ring.size();
    }
    ByteBuffer buffer = ByteBuffer.allocate(44 + 4 * rings.size() + 16 * numPoints).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(ShapeType.POLYGON.id);
    for (int i = 0; i < 4; i++) {
      buffer.putDouble(0);
    }
    buffer.putInt(rings.size()).putInt(numPoints);
    int partStart = 0;
    for (List<Coordinate> ring : rings) {
      buffer.putInt(partStart);
      partStart += ring.size();
    }
    for (List<Coordinate> ring : rings) {
      for (Coordinate coord : ring) {
        buffer.putDouble(coord.x).putDouble(coord.y);
      }
    }
    return buffer.position(4);
  }

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

  private static Geometry read(List<List<Coordinate>> rings) {
    return (Geometry) new ShapefilePolygonHandler().read(polygonRecord(rings), ShapeType.POLYGON, false);
  }

  private static void assertSameAsGeotools(List<List<Coordinate>> rings) {
    Geometry expected =
      (Geometry) new PolygonHandler(JTS_FACTORY).read(polygonRecord(rings), ShapeType.POLYGON, false);
    Geometry actual = read(rings);
    assertTrue(expected.equalsExact(actual), () -> "expected " + expected + " got " + actual);
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
    assertSameAsGeotools(rings);
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10), List.of(hole(1, 1, 3, 3), hole(5, 5, 7, 7)))
    ), rings);
  }

  @Test
  void testHolesSplitBetweenOuterRings() {
    assertSameAsGeotools(List.of(
      hole(5, 5, 7, 7),
      shell(0, 0, 10, 10),
      hole(25, 5, 27, 7),
      shell(20, 0, 30, 10),
      hole(1, 1, 3, 3),
      hole(21, 1, 23, 3)
    ));
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
    assertSameAsGeotools(rings);
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
    assertSameAsGeotools(rings);
    assertReads(newMultiPolygon(
      newPolygon(shell(0, 0, 10, 10)),
      newPolygon(shell(30, 0, 40, 10)),
      newPolygon(hole(20, 20, 25, 25))
    ), rings);
  }

  @Test
  void testEveryHoleGoesIntoOnlyOuterRing() {
    // a hole outside the only outer ring makes the polygon invalid, as the record describes
    assertSameAsGeotools(List.of(
      newCoordinateList(0, 0, 0, 10, 5, 10, 5, 5, 10, 5, 10, 0, 0, 0),
      hole(6, 6, 8, 8),
      hole(20, 20, 21, 21)
    ));
  }

  @Test
  void testOnlyHoles() {
    assertSameAsGeotools(List.of(
      hole(0, 0, 1, 1),
      hole(5, 5, 6, 6)
    ));
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
    assertSameAsGeotools(rings);
    assertReads(newMultiPolygon(
      newPolygon(shell(-10, -10, 0, 0)),
      newPolygon(shell(0, 0, 10, 10), List.of(triangle))
    ), rings);
  }

  @Test
  void testHoleTouchingLargeOuterRingVertex() {
    var triangle = newCoordinateList(0, 0, 50, 20, 20, 50, 0, 0);
    assertSameAsGeotools(List.of(
      denseShell(-100, -100, 0, 0, 50),
      denseShell(0, 0, 100, 100, 50),
      triangle
    ));
    assertSameAsGeotools(List.of(
      denseShell(0, 0, 100, 100, 50),
      triangle,
      hole(10, 60, 15, 65)
    ));
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
    assertSameAsGeotools(rings);
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
    assertSameAsGeotools(rings);
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
    assertSameAsGeotools(rings);
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
    var handler = new ShapefilePolygonHandler();
    ByteBuffer onlyShortRings = polygonRecord(List.of(
      List.of(),
      newCoordinateList(1, 1, 2, 2, 1, 1)
    ));
    assertThrows(IllegalArgumentException.class, () -> handler.read(onlyShortRings, ShapeType.POLYGON, false));
    ByteBuffer noRings = polygonRecord(List.of());
    assertThrows(IllegalArgumentException.class, () -> handler.read(noRings, ShapeType.POLYGON, false));
  }

  @Test
  void testNullRecord() {
    ByteBuffer expectedBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).position(4);
    ByteBuffer actualBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).position(4);
    Object expected = new PolygonHandler(JTS_FACTORY).read(expectedBuffer, ShapeType.NULL, false);
    Object actual = new ShapefilePolygonHandler().read(actualBuffer, ShapeType.NULL, false);
    assertEquals(expected, actual);
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
    for (int i = 0; i < 50; i++) {
      // inside a tooth, then in the gap after it (outside the comb, but inside its envelope)
      rings.add(hole(2 * i + 0.25, 12 + i % 5, 2 * i + 0.75, 12.5 + i % 5));
      rings.add(hole(2 * i + 1.25, 15, 2 * i + 1.75, 15.5));
    }
    assertSameAsGeotools(rings);
  }

  @Test
  void testHoleLevelWithOuterRingVertices() {
    // a ray from each hole's first point (y=10) passes through every vertex where the teeth meet the body
    List<List<Coordinate>> rings = new ArrayList<>(List.of(
      comb(50),
      shell(1000, 0, 1010, 10)
    ));
    for (int i = 0; i < 50; i += 7) {
      rings.add(hole(2 * i + 0.25, 10, 2 * i + 0.75, 10.5));
    }
    assertSameAsGeotools(rings);
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
