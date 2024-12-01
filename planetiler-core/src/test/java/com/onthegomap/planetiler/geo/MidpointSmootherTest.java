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
      TestUtils.round(new MidpointSmoother(new double[]{0.5}).setIters(1).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING(0 0, 8 0, 10 2, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10, 0 10); LINESTRING(0 0, 8 0, 10 2, 10 8, 8 10, 0 10)",
    "POLYGON((0 0, 10 0, 10 10, 0 0)); POLYGON((2 0, 8 0, 10 2, 10 8, 8 8, 2 2, 2 0))",
    "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)); POLYGON((2 0, 8 0, 10 2, 10 8, 8 10, 2 10, 0 8, 0 2, 2 0))",
  }, delimiter = ';')
  void testDualMidpointSmooth(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(new MidpointSmoother(new double[]{0.2, 0.8}).setIters(1).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING(0 0, 6.4 0, 8.4 0.4, 9.6 1.6, 10 3.6, 10 10)",
    "POLYGON((0 0, 10 0, 10 10, 0 0)); POLYGON((3.2 0, 6.8 0, 8.4 0.4, 9.6 1.6, 10 3.2, 10 6.8, 9.6 8, 8.4 8, 6.8 6.8, 3.2 3.2, 2 1.6, 2 0.4, 3.2 0))",
  }, delimiter = ';')
  void testMultiPassSmooth(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(new MidpointSmoother(new double[]{0.2, 0.8}).setIters(2).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING(5 0, 10 5)",
    "LINESTRING(0 0, 10 0, 10 10, 0 10); LINESTRING(5 0, 10 5, 5 10)",
  }, delimiter = ';')
  void testDontIncludeEndpointsMidpoint(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    assertEquals(
      TestUtils.round(reader.read(outWKT)),
      TestUtils.round(
        new MidpointSmoother(new double[]{0.5}).setIters(1).includeLineEndpoints(false).apply(reader.read(inWKT)))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING(2 0, 8 0, 10 2, 10 8)",
    "LINESTRING(0 0, 10 0, 10 10, 0 10); LINESTRING(2 0, 8 0, 10 2, 10 8, 8 10, 2 10)",
  }, delimiter = ';')
  void testDontIncludeEndpointsDualMidpoint(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    assertEquals(
      TestUtils.round(reader.read(outWKT)),
      TestUtils.round(
        new MidpointSmoother(new double[]{0.2, 0.8}).setIters(1).includeLineEndpoints(false).apply(reader.read(inWKT)))
    );
  }
}
