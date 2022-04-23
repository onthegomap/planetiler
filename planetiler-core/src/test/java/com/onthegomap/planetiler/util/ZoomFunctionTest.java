package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ZoomFunctionTest {

  private static <T> void assertValueInRange(ZoomFunction<?> fn, int min, int max, T value) {
    for (int i = min; i <= max; i++) {
      assertEquals(value, fn.apply(i), "z" + i);
    }
  }

  @Test
  void testNullBelowZoom() {
    var fn = ZoomFunction.minZoom(10, "value");
    assertValueInRange(fn, 0, 9, null);
    assertValueInRange(fn, 10, 14, "value");
  }

  @Test
  void testNullAboveZoom() {
    var fn = ZoomFunction.maxZoom(10, "value");
    assertValueInRange(fn, 0, 10, "value");
    assertValueInRange(fn, 11, 14, null);
  }

  @Test
  void testValueInRange() {
    var fn = ZoomFunction.zoomRange(10, 12, "value");
    assertValueInRange(fn, 0, 9, null);
    assertValueInRange(fn, 10, 12, "value");
    assertValueInRange(fn, 13, 14, null);
  }

  @Test
  void testMultipleThresholds() {
    var fn = ZoomFunction.fromMaxZoomThresholds(Map.of(
      3, "3",
      5, "5"
    ));
    assertValueInRange(fn, 0, 3, "3");
    assertValueInRange(fn, 4, 5, "5");
    assertValueInRange(fn, 6, 14, null);
  }

  @Test
  void testConvertMetersToPixels() {
    var fn = ZoomFunction.meterThresholds()
      .put(7, 20_000)
      .put(8, 14_000)
      .put(11, 8_000);
    assertNull(fn.apply(6));
    assertEquals(16, fn.apply(7).doubleValue(), 1d);
    assertEquals(23, fn.apply(8).doubleValue(), 1d);
    assertNull(fn.apply(9));
    assertNull(fn.apply(10));
    assertEquals(105, fn.apply(11).doubleValue(), 1d);
    assertNull(fn.apply(12));
  }
}
