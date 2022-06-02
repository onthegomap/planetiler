package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.geo.GeoUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.util.AffineTransformation;

class GeoUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "0,0, 0.5,0.5",
    "0, -180, 0, 0.5",
    "0, 180, 1, 0.5",
    "0, " + (180 - 1e-7) + ", 1, 0.5",
    "45, 0, 0.5, 0.359725",
    "-45, 0, 0.5, " + (1 - 0.359725),
    "86, -198, -0.05, -0.03391287",
    "-86, 198, 1.05, 1.03391287",
  })
  void testWorldCoords(double lat, double lon, double worldX, double worldY) {
    assertEquals(worldY, getWorldY(lat), 1e-5);
    assertEquals(worldX, getWorldX(lon), 1e-5);
    long encoded = encodeFlatLocation(lon, lat);
    assertEquals(worldY, decodeWorldY(encoded), 1e-5);
    assertEquals(worldX, decodeWorldX(encoded), 1e-5);

    Point input = newPoint(lon, lat);
    Point expected = newPoint(worldX, worldY);
    Geometry actual = latLonToWorldCoords(input);
    assertEquals(round(expected), round(actual));

    Geometry roundTripped = worldToLatLonCoords(actual);
    assertEquals(round(input), round(roundTripped));
  }

  @Test
  void testPolygonToLineString() throws GeometryException {
    assertEquals(newLineString(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ), GeoUtils.polygonToLineString(rectangle(
      0, 1
    )));
  }

  @Test
  void testMultiPolygonToLineString() throws GeometryException {
    assertEquals(newLineString(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ), GeoUtils.polygonToLineString(newMultiPolygon(rectangle(
      0, 1
    ))));
  }

  @Test
  void testLineRingToLineString() throws GeometryException {
    assertEquals(newLineString(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ), GeoUtils.polygonToLineString(rectangle(
      0, 1
    ).getExteriorRing()));
  }

  @Test
  void testComplexPolygonToLineString() throws GeometryException {
    assertEquals(newMultiLineString(
      newLineString(
        0, 0,
        3, 0,
        3, 3,
        0, 3,
        0, 0
      ), newLineString(
        1, 1,
        2, 1,
        2, 2,
        1, 2,
        1, 1
      )
    ), GeoUtils.polygonToLineString(newPolygon(
      rectangleCoordList(
        0, 3
      ), List.of(rectangleCoordList(
        1, 2
      )))));
  }

  @ParameterizedTest
  @CsvSource({
    "0, 156543",
    "8, 611",
    "14, 9",
  })
  void testMetersPerPixel(int zoom, double meters) {
    assertEquals(meters, metersPerPixelAtEquator(zoom), 1);
  }

  @Test
  void testIsConvexTriangle() {
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      0, 1,
      0, 0
    ));
  }

  @Test
  void testIsConvexRectangle() {
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
  }

  @Test
  void testBarelyConvexRectangle() {
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 0.5,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.4, 0.4,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.7, 0.7,
      0, 0
    ));
  }

  @Test
  void testConcaveRectangleDoublePoints() {
    assertConcave(true, newLinearRing(
      0, 0,
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0,
      0, 0
    ));
  }

  @Test
  void testBarelyConcaveRectangle() {
    assertConcave(false, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.51, 0.5,
      0, 0
    ));
  }

  @Test
  void test5PointsConcave() {
    assertConcave(false, newLinearRing(
      0, 0,
      0.5, 0.1,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(false, newLinearRing(
      0, 0,
      1, 0,
      0.9, 0.5,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(false, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 0.9,
      0, 1,
      0, 0
    ));
    assertConcave(false, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0.1, 0.5,
      0, 0
    ));
  }

  @Test
  void test5PointsColinear() {
    assertConcave(true, newLinearRing(
      0, 0,
      0.5, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 0.5,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 1,
      0, 1,
      0, 0
    ));
    assertConcave(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0.5,
      0, 0
    ));
  }

  private static void assertConcave(boolean isConcave, LinearRing ring) {
    for (double rotation : new double[]{0, 90, 180, 270}) {
      LinearRing rotated = (LinearRing) AffineTransformation.rotationInstance(Math.toRadians(rotation)).transform(ring);
      for (boolean flip : new boolean[]{false, true}) {
        LinearRing flipped = flip ? (LinearRing) AffineTransformation.scaleInstance(-1, 1).transform(rotated) : rotated;
        for (boolean reverse : new boolean[]{false, true}) {
          LinearRing reversed = reverse ? flipped.reverse() : flipped;
          assertEquals(isConcave, isConcave(reversed),
            "rotation=" + rotation + " flip=" + flip + " reverse=" + reverse);
          assertEquals(!isConcave, isConvex(reversed),
            "rotation=" + rotation + " flip=" + flip + " reverse=" + reverse);
        }
      }
    }
  }

  @Test
  void testCombineEmpty() {
    assertEquals(EMPTY_GEOMETRY, GeoUtils.combine());
  }

  @Test
  void testCombineOne() {
    assertEquals(newLineString(0, 0, 1, 1), GeoUtils.combine(newLineString(0, 0, 1, 1)));
  }

  @Test
  void testCombineTwo() {
    assertEquals(GeoUtils.JTS_FACTORY.createGeometryCollection(new Geometry[]{
      newLineString(0, 0, 1, 1),
      newLineString(2, 2, 3, 3)
    }), GeoUtils.combine(
      newLineString(0, 0, 1, 1),
      newLineString(2, 2, 3, 3)
    ));
  }

  @Test
  void testCombineNested() {
    assertEquals(GeoUtils.JTS_FACTORY.createGeometryCollection(new Geometry[]{
      newLineString(0, 0, 1, 1),
      newLineString(2, 2, 3, 3),
      newLineString(4, 4, 5, 5)
    }), GeoUtils.combine(
      GeoUtils.combine(
        newLineString(0, 0, 1, 1),
        newLineString(2, 2, 3, 3)
      ),
      newLineString(4, 4, 5, 5)
    ));
  }
}
