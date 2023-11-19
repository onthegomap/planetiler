package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InterpolatorTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLinear(boolean clamp) {
    var interpolator = Scales.linear()
      .put(0, 10)
      .put(1, 20)
      .clamp(clamp);
    assertTransform(interpolator, 0, 10);
    assertTransform(interpolator, 1, 20);
    assertTransform(interpolator, 0.5, 15);
    // clamp
    assertClose(clamp ? 20 : 30, interpolator.applyAsDouble(2));
    assertClose(clamp ? 1 : 2, interpolator.invert().applyAsDouble(30));
    assertClose(clamp ? 10 : 0, interpolator.applyAsDouble(-1));
    assertClose(clamp ? 0 : -1, interpolator.invert().applyAsDouble(0));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLog(boolean clamp) {
    var interpolator = Scales.log(10)
      .put(10, 1d)
      .put(1_000, 2d)
      .clamp(clamp);
    assertTransform(interpolator, 10, 1);
    assertTransform(interpolator, 100, 1.5);
    assertTransform(interpolator, 1_000, 2);
    // clamp
    assertClose(clamp ? 1 : 0.5, interpolator.applyAsDouble(1));
    assertClose(clamp ? 10 : 1, interpolator.invert().applyAsDouble(0.5));
    assertClose(clamp ? 2 : 2.5, interpolator.applyAsDouble(10_000));
    assertClose(clamp ? 1_000 : 10_000, interpolator.invert().applyAsDouble(2.5));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLog2(boolean clamp) {
    var interpolator = Scales.log(2)
      .put(2, 1d)
      .put(8, 2d)
      .clamp(clamp);
    assertTransform(interpolator, 2, 1);
    assertTransform(interpolator, 4, 1.5);
    assertTransform(interpolator, 8, 2);
    // clamp
    assertClose(clamp ? 1 : 0.5, interpolator.applyAsDouble(1));
    assertClose(clamp ? 2 : 1, interpolator.invert().applyAsDouble(0.5));
    assertClose(clamp ? 2 : 2.5, interpolator.applyAsDouble(16));
    assertClose(clamp ? 8 : 16, interpolator.invert().applyAsDouble(2.5));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testPower(boolean clamp) {
    var interpolator = Scales.power(2)
      .put(Math.sqrt(2), 1d)
      .put(Math.sqrt(4), 2d)
      .clamp(clamp);
    assertTransform(interpolator, Math.sqrt(2), 1);
    assertTransform(interpolator, Math.sqrt(3), 1.5);
    assertTransform(interpolator, Math.sqrt(4), 2);
    // clamp
    assertClose(clamp ? 1 : 0.5, interpolator.applyAsDouble(1));
    assertClose(clamp ? Math.sqrt(2) : 1, interpolator.invert().applyAsDouble(0.5));
    assertClose(clamp ? 2 : 2.5, interpolator.applyAsDouble(Math.sqrt(5)));
    assertClose(clamp ? Math.sqrt(4) : Math.sqrt(5), interpolator.invert().applyAsDouble(2.5));
  }

  @Test
  void testNegativePower() {
    var interpolator = Scales.power(-1)
      .put(1, 0d)
      .put(2, 1d);
    assertTransform(interpolator, 1, 0);
    assertTransform(interpolator, 1.5, 2d / 3);
    assertTransform(interpolator, 2, 1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSqrt(boolean clamp) {
    var interpolator = Scales.sqrt()
      .put(4, 1d)
      .put(16, 2d)
      .clamp(clamp);
    assertTransform(interpolator, 4, 1);
    assertTransform(interpolator, 9, 1.5);
    assertTransform(interpolator, 16, 2);
    // clamp
    assertClose(clamp ? 1 : 0.5, interpolator.applyAsDouble(1));
    assertClose(clamp ? 4 : 1, interpolator.invert().applyAsDouble(0.5));
    assertClose(clamp ? 2 : 2.5, interpolator.applyAsDouble(25));
    assertClose(clamp ? 16 : 25, interpolator.invert().applyAsDouble(2.5));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMultipartDescending(boolean clamp) {
    var interpolator = Scales.linear()
      .put(4, 1d)
      .put(2, 2d)
      .put(1, 4d)
      .clamp(clamp);
    assertTransform(interpolator, 4, 1);
    assertTransform(interpolator, 3, 1.5);
    assertTransform(interpolator, 2, 2);
    assertTransform(interpolator, 1.5, 3);
    assertTransform(interpolator, 1, 4);
    // clamp
    assertClose(clamp ? 1 : 0.5, interpolator.applyAsDouble(5));
    assertClose(clamp ? 4 : 5, interpolator.invert().applyAsDouble(0.5));
    assertClose(clamp ? 4 : 6, interpolator.applyAsDouble(0));
    assertClose(clamp ? 1 : 0, interpolator.invert().applyAsDouble(6));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMultipartAscending(boolean clamp) {
    var interpolator = Scales.linear()
      .put(1, 4d)
      .put(2, 2d)
      .put(4, 1d)
      .clamp(clamp);
    assertTransform(interpolator, 1, 4);
    assertTransform(interpolator, 1.5, 3);
    assertTransform(interpolator, 2, 2);
    assertTransform(interpolator, 3, 1.5);
    assertTransform(interpolator, 4, 1);
    // clamp
    assertClose(clamp ? 1 : 0.5, interpolator.invert().applyAsDouble(5));
    assertClose(clamp ? 4 : 5, interpolator.applyAsDouble(0.5));
    assertClose(clamp ? 4 : 6, interpolator.invert().applyAsDouble(0));
    assertClose(clamp ? 1 : 0, interpolator.applyAsDouble(6));
  }

  @Test
  void testUnknown() {
    var interpolator = Scales.linear()
      .put(0, 10d)
      .put(1, 20d)
      .defaultValue(100d);
    assertEquals(100, interpolator.applyAsDouble(Double.NaN));
  }

  @Test
  void testThreshold() {
    var scale = Scales.threshold("a")
      .putAbove(1.5, "b")
      .putAbove(3, "c");
    assertEquals("a", scale.apply(1.4));
    assertEquals("b", scale.apply(1.5));
    assertEquals("b", scale.apply(2.99));
    assertEquals("c", scale.apply(3));
    assertEquals("c", scale.apply(3.1));

    testInvert(scale, "a", Double.NEGATIVE_INFINITY, 1.5);
    testInvert(scale, "b", 1.5, 3);
    testInvert(scale, "c", 3, Double.POSITIVE_INFINITY);
    testInvert(scale, "d", Double.NaN, Double.NaN);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testQuantize(boolean list) {
    var scale = list ?
      Scales.quantize(0, 3, List.of("a", "b", "c")) :
      Scales.quantize(0, 3, "a", "b", "c");
    assertEquals("a", scale.apply(0));
    assertEquals("a", scale.apply(0.99));
    assertEquals("b", scale.apply(1));
    assertEquals("b", scale.apply(1.01));
    assertEquals("b", scale.apply(1.99));
    assertEquals("c", scale.apply(2));
    assertEquals("c", scale.apply(2.01));
    assertEquals("c", scale.apply(99));
  }

  private static <V> void testInvert(Scales.ThresholdScale<V> scale, V val, double min, double max) {
    assertEquals(min, scale.invertMin(val));
    assertEquals(max, scale.invertMax(val));
    assertEquals(max, scale.invertMax(val));
  }

  private static void assertClose(double expected, double actual) {
    assertEquals(expected, actual, 1e-10);
  }

  private static void assertTransform(Scales.DoubleContinuous interp, double x, double y) {
    assertClose(y, interp.applyAsDouble(x));
    assertClose(x, interp.invert().applyAsDouble(y));
  }
}
