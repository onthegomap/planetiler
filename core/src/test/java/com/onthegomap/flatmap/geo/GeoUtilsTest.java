package com.onthegomap.flatmap.geo;

import static com.onthegomap.flatmap.TestUtils.*;
import static com.onthegomap.flatmap.geo.GeoUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

public class GeoUtilsTest {

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
  public void testWorldCoords(double lat, double lon, double worldX, double worldY) {
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
  public void testPolygonToLineString() throws GeometryException {
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
  public void testMultiPolygonToLineString() throws GeometryException {
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
  public void testLineRingToLineString() throws GeometryException {
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
  public void testComplexPolygonToLineString() throws GeometryException {
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
  public void testMetersPerPixel(int zoom, double meters) {
    assertEquals(meters, metersPerPixelAtEquator(zoom), 1);
  }
}
