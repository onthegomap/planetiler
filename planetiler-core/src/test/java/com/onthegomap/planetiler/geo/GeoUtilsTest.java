package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.geo.GeoUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.stats.Stats;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

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
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      0, 1,
      0, 0
    ));
  }

  @Test
  void testIsConvexRectangle() {
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
  }

  @Test
  void testBarelyConvexRectangle() {
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 0.5,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.4, 0.4,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.7, 0.7,
      0, 0
    ));
  }

  @Test
  void testConcaveRectangleDoublePoints() {
    assertConvex(true, newLinearRing(
      0, 0,
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0,
      0, 0
    ));
  }

  @Test
  void testBarelyConcaveTriangle() {
    assertConvex(false, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.51, 0.5,
      0, 0
    ));
  }

  @Test
  void testAllowVerySmallConcavity() {
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5001, 0.5,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 0.4999,
      0, 0
    ));
  }

  @Test
  void test5PointsConcave() {
    assertConvex(false, newLinearRing(
      0, 0,
      0.5, 0.1,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(false, newLinearRing(
      0, 0,
      1, 0,
      0.9, 0.5,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(false, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 0.9,
      0, 1,
      0, 0
    ));
    assertConvex(false, newLinearRing(
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
    assertConvex(true, newLinearRing(
      0, 0,
      0.5, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 0.5,
      1, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0.5, 1,
      0, 1,
      0, 0
    ));
    assertConvex(true, newLinearRing(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0.5,
      0, 0
    ));
  }

  private static void assertConvex(boolean isConvex, LinearRing ring) {
    for (double rotation : new double[]{0, 90, 180, 270}) {
      LinearRing rotated = (LinearRing) AffineTransformation.rotationInstance(Math.toRadians(rotation)).transform(ring);
      for (boolean flip : new boolean[]{false, true}) {
        LinearRing flipped = flip ? (LinearRing) AffineTransformation.scaleInstance(-1, 1).transform(rotated) : rotated;
        for (boolean reverse : new boolean[]{false, true}) {
          LinearRing reversed = reverse ? flipped.reverse() : flipped;
          for (double scale : new double[]{1, 1e-2, 1 / Math.pow(2, 14) / 4096}) {
            LinearRing scaled = (LinearRing) AffineTransformation.scaleInstance(scale, scale).transform(reversed);
            assertEquals(isConvex, isConvex(scaled),
              "rotation=" + rotation + " flip=" + flip + " reverse=" + reverse + " scale=" + scale);
          }
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

  @Test
  void testSnapAndFixIssue511() throws ParseException, GeometryException {
    var result = GeoUtils.snapAndFixPolygon(new WKTReader().read(
      """
        MULTIPOLYGON (((198.83750000000003 46.07500000000004, 199.0625 46.375, 199.4375 46.0625, 199.5 46.43750000000001, 199.5625 46, 199.3125 45.5, 198.8912037037037 46.101851851851876, 198.83750000000003 46.07500000000004)), ((198.43750000000003 46.49999999999999, 198.5625 46.43750000000001, 198.6875 46.25, 198.1875 46.25, 198.43750000000003 46.49999999999999)), ((198.6875 46.25, 198.81249999999997 46.062500000000014, 198.6875 46.00000000000002, 198.6875 46.25)), ((196.55199579831933 46.29359243697479, 196.52255639097743 46.941259398496236, 196.5225563909774 46.941259398496236, 196.49999999999997 47.43750000000001, 196.875 47.125, 197 47.5625, 197.47880544905414 46.97729334004497, 197.51505401161464 46.998359569801956, 197.25 47.6875, 198.0625 47.6875, 198.5 46.625, 198.34375 46.546875, 198.34375000000003 46.54687499999999, 197.875 46.3125, 197.875 46.25, 197.875 46.0625, 197.82894736842107 46.20065789473683, 197.25 46.56250000000001, 197.3125 46.125, 196.9375 46.1875, 196.9375 46.21527777777778, 196.73250000000002 46.26083333333334, 196.5625 46.0625, 196.55199579831933 46.29359243697479)), ((196.35213414634146 45.8170731707317, 197.3402027027027 45.93108108108108, 197.875 45.99278846153846, 197.875 45.93750000000002, 197.93749999999997 45.99999999999999, 197.9375 46, 197.90625 45.96874999999999, 197.90625 45.96875, 196.75000000000006 44.81250000000007, 197.1875 45.4375, 196.3125 45.8125, 196.35213414634146 45.8170731707317)), ((195.875 46.124999999999986, 195.8125 46.5625, 196.5 46.31250000000001, 195.9375 46.4375, 195.875 46.124999999999986)), ((196.49999999999997 46.93749999999999, 196.125 46.875, 196.3125 47.125, 196.49999999999997 46.93749999999999)))
        """),
      Stats.inMemory(), "test");
    assertTrue(result.isValid());
    assertEquals(3.083984375, result.getArea(), 1e-5);
  }

  @Test
  void testSnapAndFixIssue546() throws GeometryException, ParseException {
    var orig = new WKTReader().read(
      """
        POLYGON(
          (
            0 0,
            2 0,
            2 1,
            0 1,
            0 0
          ),
          (
            1.19053596444428 0.9029693332377,
            1.06439281777784 0.92184484124482,
            1.42787640888855 0.8257737683016,
            0.94579086222257 0.5617504680322,
            1.19053596444428 0.9029693332377
          )
        )
        """);
    var result = GeoUtils.snapAndFixPolygon(orig, Stats.inMemory(), "test");
    var point = newPoint(1.14602002962965, 0.76978969252622);
    assertTrue(result.isValid());
    assertFalse(result.contains(point));
  }

  @Test
  void testSnapAndFixIssue546_2() throws GeometryException, ParseException {
    var orig = new WKTReader().read(
      """
        POLYGON(
          (
            1.19053596444428 0.9029693332377,
            1.06439281777784 0.92184484124482,
            1.42787640888855 0.8257737683016,
            0.94579086222257 0.5617504680322,
            1.19053596444428 0.9029693332377
          )
        )
        """);
    var result = GeoUtils.snapAndFixPolygon(orig, Stats.inMemory(), "test");
    assertTrue(result.isValid());
    assertFalse(Orientation.isCCWArea(result.getCoordinates()));
  }

  @ParameterizedTest
  @CsvSource({
    "1,0,0",
    "1,10,0",
    "1,255,0",

    "0.5,0,0",
    "0.5,128,0",
    "0.5,129,1",
    "0.5,256,1",

    "0.25,0,0",
    "0.25,128,1",
    "0.25,129,2",
    "0.25,256,2",
  })
  void minZoomForPixelSize(double worldGeometrySize, double minPixelSize, int expectedMinZoom) {
    assertEquals(expectedMinZoom, GeoUtils.minZoomForPixelSize(worldGeometrySize, minPixelSize));
  }

  @Test
  void minZoomForPixelSizesAtZ9_10() {
    assertEquals(10, GeoUtils.minZoomForPixelSize(3.1 / (256 << 10), 3));
    assertEquals(9, GeoUtils.minZoomForPixelSize(6.1 / (256 << 10), 3));
  }

  @Test
  void getLongestLine() {
    var line1 = newLineString(0, 0, 1, 1);
    var line2 = newLineString(0, 0, 2, 2);
    assertNull(GeoUtils.getLongestLine(newMultiLineString()));
    assertEquals(line1, GeoUtils.getLongestLine(newMultiLineString(line1)));
    assertEquals(line2, GeoUtils.getLongestLine(newMultiLineString(line1, line2)));
  }


  @Test
  void areaInMeters() {
    assertEquals(0, GeoUtils.areaInMeters(newLineString(0, 0, 1, 1)));
    assertEquals(0, GeoUtils.areaInMeters(newPoint(0, 1)));
    assertEquals(111_194, Math.sqrt(GeoUtils.areaInMeters(rectangle(-0.5, 0.5))), 1d);
    assertEquals(GeoUtils.areaInMeters(rectangle(0, 3)) - GeoUtils.areaInMeters(rectangle(1, 2)),
      GeoUtils.areaInMeters(newPolygon(rectangleCoordList(0, 3), List.of(rectangleCoordList(1, 2)))), 1d);
    assertEquals(96_963, Math.sqrt(GeoUtils.areaInMeters(rectangle(40, 41))), 1d);
    assertEquals(10_387, Math.sqrt(GeoUtils.areaInMeters(rectangle(89, 90))), 1d);
    assertEquals(10_387, Math.sqrt(GeoUtils.areaInMeters(rectangle(-89, -90))), 1d);
    assertEquals(5.1e14,
      GeoUtils.areaInMeters(newPolygon(-180, -90, 180, -90, 180, 90, -180, 90, -180, -90)), 1e11);
  }

  @Test
  void lengthInMeters() {
    assertEquals(0, GeoUtils.lengthInMeters(rectangle(0, 1)));
    assertEquals(0, GeoUtils.lengthInMeters(newPoint(0, 0)));
    assertEquals(97_129, GeoUtils.lengthInMeters(newLineString(
      -75.343, 39.984,
      -75.534, 39.123
    )), 1);
    assertEquals(97_129 * 3, GeoUtils.lengthInMeters(newLineString(
      -75.343, 39.984,
      -75.534, 39.123,
      -75.343, 39.984,
      -75.534, 39.123
    )), 1);
    assertEquals(15051, GeoUtils.lengthInMeters(newLineString(
      47.234, 24.235,
      47.234 + 0.1, 24.235 - 0.1)), 1d);
  }
}
