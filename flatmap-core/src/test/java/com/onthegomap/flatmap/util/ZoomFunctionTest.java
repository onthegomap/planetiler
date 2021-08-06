package com.onthegomap.flatmap.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class ZoomFunctionTest {

  private static <T> void assertValueInRange(ZoomFunction fn, int min, int max, T value) {
    for (int i = min; i <= max; i++) {
      assertEquals(value, fn.apply(i), "z" + i);
    }
  }

//  private static void assertDoubleValueInRange(ZoomFunction.OfDouble fn, int min, int max, double value) {
//    for (int i = min; i <= max; i++) {
//      assertEquals(value, fn.applyAsDouble(i), 1e-5, "z" + i);
//    }
//  }
//
//  private static void assertIntValueInRange(ZoomFunction.OfInt fn, int min, int max, double value) {
//    for (int i = min; i <= max; i++) {
//      assertEquals(value, fn.applyAsInt(i), "z" + i);
//    }
//  }

  @Test
  public void testNullBelowZoom() {
    var fn = ZoomFunction.minZoom(10, "value");
    assertValueInRange(fn, 0, 9, null);
    assertValueInRange(fn, 10, 14, "value");
  }

  @Test
  public void testNullAboveZoom() {
    var fn = ZoomFunction.maxZoom(10, "value");
    assertValueInRange(fn, 0, 10, "value");
    assertValueInRange(fn, 11, 14, null);
  }

  @Test
  public void testValueInRange() {
    var fn = ZoomFunction.zoomRange(10, 12, "value");
    assertValueInRange(fn, 0, 9, null);
    assertValueInRange(fn, 10, 12, "value");
    assertValueInRange(fn, 13, 14, null);
  }

  @Test
  public void testMultipleThresholds() {
    var fn = ZoomFunction.fromMaxZoomThresholds(Map.of(
      3, "3",
      5, "5"
    ));
    assertValueInRange(fn, 0, 3, "3");
    assertValueInRange(fn, 4, 5, "5");
    assertValueInRange(fn, 6, 14, null);
  }

  @Test
  public void testConvertMetersToPixels() {
    var fn = ZoomFunction.fromMaxZoomThresholds(Map.of(
      7, 20_000,
      8, 14_000,
      11, 8_000
    )).andThen(ZoomFunction.metersToPixelsAtEquator());
    assertEquals(8, fn.apply(6), 1d);
    assertEquals(16, fn.apply(7), 1d);
    assertEquals(23, fn.apply(8), 1d);
    assertEquals(26, fn.apply(9), 1d);
    assertEquals(52, fn.apply(10), 1d);
    assertEquals(105, fn.apply(11), 1d);
    assertNull(fn.apply(12));
  }

//  @Test
//  public void testMultipleThresholds() {
//    var fn = ZoomFunction.ranges(
//      ZoomFunction.range(5, 7, "value"),
//      ZoomFunction.range(10, 12, "value13")
//    );
//    assertValueInRange(fn, 0, 4, null);
//    assertValueInRange(fn, 5, 7, "value");
//    assertValueInRange(fn, 8, 9, null);
//    assertValueInRange(fn, 10, 12, "value13");
//    assertValueInRange(fn, 13, 14, null);
//  }
//
//  @Test
//  public void testDoubleMetersToPixels() {
//    var fn = ZoomFunction.metersToPixelsAtEquator(12,
//      doubleRange(0, 7, 20_000),
//      doubleRange(8, 8, 14_000),
//      doubleRange(9, 11, 8_000)
//    );
//    assertEquals(8, fn.applyAsDouble(6), 1d);
//    assertEquals(16, fn.applyAsDouble(7), 1d);
//    assertEquals(23, fn.applyAsDouble(8), 1d);
//    assertEquals(26, fn.applyAsDouble(9), 1d);
//    assertEquals(52, fn.applyAsDouble(10), 1d);
//    assertEquals(105, fn.applyAsDouble(11), 1d);
//    assertEquals(12, fn.applyAsDouble(12), 1d);
//  }
}
