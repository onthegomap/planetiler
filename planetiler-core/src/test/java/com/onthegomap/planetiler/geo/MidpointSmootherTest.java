package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class MidpointSmootherTest {

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING(0 0, 5 0, 10 5, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10, 0 10); LINESTRING(0 0, 5 0, 10 5, 5 10, 0 10)",
    "POLYGON((0 0, 10 0, 10 10, 0 0)); POLYGON((5 0, 10 5, 5 5, 5 0))",
    "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)); POLYGON((5 0, 10 5, 5 10, 0 5, 5 0))",
  }, delimiter = ';')
  void testMidpointSmooth(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(new MidpointSmoother().setIters(1).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING(0 0, 2.5 0, 7.5 2.5, 10 7.5, 10 10)",
    "POLYGON((0 0, 10 0, 10 10, 0 0)); POLYGON ((7.5 2.5, 7.5 5, 5 2.5, 7.5 2.5))",
    "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)); POLYGON ((7.5 2.5, 7.5 7.5, 2.5 7.5, 2.5 2.5, 7.5 2.5))",
  }, delimiter = ';')
  void testMidpointSmoothTwice(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(new MidpointSmoother().setIters(2).apply(in))
    );
  }
}
