package com.onthegomap.flatmap.geo;

import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.round;
import static com.onthegomap.flatmap.geo.GeoUtils.ProjectWorldCoords;
import static com.onthegomap.flatmap.geo.GeoUtils.decodeWorldX;
import static com.onthegomap.flatmap.geo.GeoUtils.decodeWorldY;
import static com.onthegomap.flatmap.geo.GeoUtils.encodeFlatLocation;
import static com.onthegomap.flatmap.geo.GeoUtils.getWorldX;
import static com.onthegomap.flatmap.geo.GeoUtils.getWorldY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

public class GeoUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "0,0, 0.5,0.5",
    "0, -180, 0, 0.5",
    "0, " + (180 - 1e-7) + ", 1, 0.5",
    "45, 0, 0.5, 0.359725",
    "-45, 0, 0.5, " + (1 - 0.359725)
  })
  public void testWorldCoords(double lat, double lon, double worldX, double worldY) {
    assertEquals(worldY, getWorldY(lat), 1e-5);
    assertEquals(worldX, getWorldX(lon), 1e-5);
    long encoded = encodeFlatLocation(lon, lat);
    assertEquals(worldY, decodeWorldY(encoded), 1e-5);
    assertEquals(worldX, decodeWorldX(encoded), 1e-5);

    Point input = newPoint(lon, lat);
    Point expected = newPoint(worldX, worldY);
    Geometry actual = ProjectWorldCoords.transform(input);
    assertEquals(round(expected), round(actual));
  }
}
