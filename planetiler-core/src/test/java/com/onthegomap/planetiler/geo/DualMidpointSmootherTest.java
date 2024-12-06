package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class DualDualMidpointSmootherTest {

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
      TestUtils.round(new DualMidpointSmoother(0.2, 0.8).setIters(1).apply(in))
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
      TestUtils.round(new DualMidpointSmoother(0.2, 0.8).setIters(2).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING (0 0, 6.4 0, 8 0.32, 8.64 0.64, 9.36 1.36, 9.68 2, 10 3.6, 10 10)",
    "POLYGON((0 0, 10 0, 10 10, 0 0)); POLYGON ((3.2 0, 6.8 0, 8.4 0.4, 9.6 1.6, 10 3.2, 10 6.8, 9.68 7.76, 9.36 8, 8.4 8, 6.8 6.8, 3.2 3.2, 2 1.6, 2 0.64, 2.24 0.32, 3.2 0))",
  }, delimiter = ';')
  void testSmoothToTolerance(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(new DualMidpointSmoother(0.2, 0.8).setIters(200).setMinVertexTolerance(0.5).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "POINT(1 1); POINT(1 1)",
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING (0 0, 5.12 0, 6.8 0.08, 8 0.32, 8.64 0.64, 9.36 1.36, 9.68 2, 9.92 3.2, 10 4.88, 10 10)",
    "POLYGON((0 0, 10 0, 10 10, 0 0)); POLYGON ((3.92 0, 6.08 0, 7.12 0.08, 8.08 0.32, 8.64 0.64, 9.36 1.36, 9.68 1.92, 9.92 2.88, 10 3.92, 10 6.08, 9.92 7.04, 9.68 7.76, 9.36 8, 8.64 8, 8.08 7.76, 7.12 7.04, 6.08 6.08, 3.92 3.92, 2.96 2.88, 2.24 1.92, 2 1.36, 2 0.64, 2.24 0.32, 2.96 0.08, 3.92 0))",
  }, delimiter = ';')
  void testSmoothToMinArea(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(new DualMidpointSmoother(0.2, 0.8).setIters(200).setMinVertexArea(0.5).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING (0 0, 9 0, 10 1, 10 10)",
    "LINESTRING(0 0, 10 0, 20 0); LINESTRING (0 0, 7.5 0, 12.5 0, 20 0)",
    "LINESTRING(0 0, 10 0, 0 0); LINESTRING (0 0, 7.5 0, 0 0)",
    "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)); POLYGON ((1 0, 9 0, 10 1, 10 9, 9 10, 1 10, 0 9, 0 1, 1 0))",
  }, delimiter = ';')
  void testSmoothWithMaxArea(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(DualMidpointSmoother.chaikin(1).setMaxArea(0.5).apply(in))
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "LINESTRING(0 0, 10 10); LINESTRING(0 0, 10 10)",
    "LINESTRING(0 0, 10 0, 10 10); LINESTRING (0 0, 9 0, 10 1, 10 10)",
    "LINESTRING(0 0, 10 0, 20 0); LINESTRING (0 0, 7.5 0, 12.5 0, 20 0)",
    "LINESTRING(0 0, 10 0, 10 5); LINESTRING (0 0, 9 0, 10 1, 10 5)",
    "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)); POLYGON ((1 0, 9 0, 10 1, 10 9, 9 10, 1 10, 0 9, 0 1, 1 0))",
  }, delimiter = ';')
  void testSmoothWithMaxOffset(String inWKT, String outWKT) throws ParseException {
    var reader = new WKTReader();
    Geometry in = reader.read(inWKT);
    Geometry out = reader.read(outWKT);
    assertEquals(
      TestUtils.round(out),
      TestUtils.round(DualMidpointSmoother.chaikin(1).setMaxOffset(Math.sqrt(0.5)).apply(in))
    );
  }
}
