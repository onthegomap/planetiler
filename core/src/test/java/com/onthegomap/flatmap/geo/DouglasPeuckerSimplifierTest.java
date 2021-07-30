package com.onthegomap.flatmap.geo;

import static com.onthegomap.flatmap.TestUtils.assertSameNormalizedFeature;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPolygon;
import static com.onthegomap.flatmap.TestUtils.rectangle;

import org.junit.jupiter.api.Test;

public class DouglasPeuckerSimplifierTest {

  @Test
  public void test() {
    var simplified = DouglasPeuckerSimplifier.simplify(newLineString(
      0, 0,
      10, 10
    ), 1);
    assertSameNormalizedFeature(
      newLineString(
        0, 0,
        10, 10
      ),
      simplified
    );
  }

  @Test
  public void testRemoveAPoint() {
    var simplified = DouglasPeuckerSimplifier.simplify(newLineString(
      0, 0,
      5, 0.9,
      10, 0
    ), 1);
    assertSameNormalizedFeature(
      newLineString(
        0, 0,
        10, 0
      ),
      simplified
    );
  }

  @Test
  public void testKeepAPoint() {
    var simplified = DouglasPeuckerSimplifier.simplify(newLineString(
      0, 0,
      5, 1.1,
      10, 0
    ), 1);
    assertSameNormalizedFeature(
      newLineString(
        0, 0,
        5, 1.1,
        10, 0
      ),
      simplified
    );
  }

  @Test
  public void testPolygonLeaveAPoint() {
    var simplified = DouglasPeuckerSimplifier.simplify(rectangle(0, 10), 20);
    assertSameNormalizedFeature(
      newPolygon(
        0, 0,
        10, 10,
        10, 0,
        0, 0
      ),
      simplified
    );
  }
}
