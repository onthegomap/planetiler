package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.collection.FeatureGroup.SORT_KEY_BITS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.function.ToIntBiFunction;
import org.junit.jupiter.api.Test;

public class SortKeyTest {

  private void assertLessThan(int a, int b) {
    if (a >= b) {
      fail(a + " was not < " + b);
    }
  }

  @Test
  public void testSingleLevel() {
    assertLessThan(
      SortKey.orderByInt(1, 0, 10).get(),
      SortKey.orderByInt(2, 0, 10).get()
    );
    assertEquals(
      SortKey.orderByInt(1, 0, 10).get(),
      SortKey.orderByInt(1, 0, 10).get()
    );
    // clamp range check
    assertEquals(
      SortKey.orderByInt(-1, 0, 10).get(),
      SortKey.orderByInt(-2, 0, 10).get()
    );
    assertEquals(
      SortKey.orderByInt(11, 0, 10).get(),
      SortKey.orderByInt(12, 0, 10).get()
    );
  }

  @Test
  public void testTwoLevel() {
    ToIntBiFunction<Integer, Integer> key = (a, b) -> SortKey
      .orderByInt(a, 0, 10)
      .thenByInt(b, 0, 10)
      .get();
    assertEquals(
      key.applyAsInt(1, 1),
      key.applyAsInt(1, 1)
    );
    assertLessThan(
      key.applyAsInt(1, 1),
      key.applyAsInt(1, 2)
    );
    assertLessThan(
      key.applyAsInt(1, 1),
      key.applyAsInt(2, 1)
    );
  }

  @Test
  public void testDescending() {
    // order by a ASC b DESC
    ToIntBiFunction<Integer, Integer> key = (a, b) -> SortKey
      .orderByInt(a, 0, 10)
      .thenByInt(b, 10, 0)
      .get();
    assertEquals(
      key.applyAsInt(0, 1),
      key.applyAsInt(0, 1)
    );
    assertLessThan(
      key.applyAsInt(10, 2),
      key.applyAsInt(10, 1)
    );
    assertLessThan(
      key.applyAsInt(1, 1),
      key.applyAsInt(2, 1)
    );
  }

  @Test
  public void testDouble() {
    ToIntBiFunction<Double, Double> key = (a, b) -> SortKey
      .orderByDouble(a, 0, 10, 10)
      .thenByDouble(b, 10, 0, 10)
      .get();
    assertEquals(
      key.applyAsInt(0d, 0d),
      key.applyAsInt(0.9d, 0.9d)
    );
    assertLessThan(
      key.applyAsInt(0.9d, 1d),
      key.applyAsInt(1.1d, 1d)
    );
    assertLessThan(
      key.applyAsInt(0d, 1.1d),
      key.applyAsInt(0d, 0.9d)
    );
  }

  @Test
  public void testBoolean() {
    ToIntBiFunction<Boolean, Boolean> key = (a, b) -> SortKey
      .orderByTruesFirst(a)
      .thenByTruesLast(b)
      .get();
    assertEquals(
      key.applyAsInt(false, false),
      key.applyAsInt(false, false)
    );
    assertLessThan(
      key.applyAsInt(true, false),
      key.applyAsInt(false, false)
    );
    assertLessThan(
      key.applyAsInt(false, false),
      key.applyAsInt(false, true)
    );
    assertLessThan(
      SortKey.orderByTruesLast(false).get(),
      SortKey.orderByTruesLast(true).get()
    );
  }

  @Test
  public void testLog() {
    ToIntBiFunction<Double, Double> key = (a, b) -> SortKey
      .orderByLog(a, 1, 1000, 3)
      .thenByLog(b, 1000, 1, 3)
      .get();
    assertEquals(
      key.applyAsInt(1d, 1d),
      key.applyAsInt(1d, 1d)
    );
    assertEquals(
      key.applyAsInt(1d, 1d),
      key.applyAsInt(9d, 9d)
    );
    assertEquals(
      key.applyAsInt(-1d, 0d),
      key.applyAsInt(0d, 0d)
    );
    assertEquals(
      key.applyAsInt(1_000d, 0d),
      key.applyAsInt(10_000d, 0d)
    );
    assertEquals(
      key.applyAsInt(11d, 11d),
      key.applyAsInt(99d, 99d)
    );
    assertLessThan(
      key.applyAsInt(9d, 1d),
      key.applyAsInt(11d, 1d)
    );
    assertLessThan(
      key.applyAsInt(99d, 1d),
      key.applyAsInt(101d, 1d)
    );
    assertLessThan(
      key.applyAsInt(1d, 11d),
      key.applyAsInt(1d, 9d)
    );
    assertLessThan(
      key.applyAsInt(1d, 101d),
      key.applyAsInt(1d, 99d)
    );

    assertThrows(AssertionError.class, () -> SortKey.orderByLog(1, 0, 2, 2));
    assertThrows(AssertionError.class, () -> SortKey.orderByLog(1, 1, 0, 2));
  }

  @Test
  public void testTooMuchResolution() {
    int max = (1 << SORT_KEY_BITS) - 1;
    int belowHalfMax = (1 << (SORT_KEY_BITS / 2)) - 1;
    int aboveHalfMax = belowHalfMax * 2;

    SortKey.orderByDouble(0, 1, 0, max);
    assertThrows(IllegalArgumentException.class, () -> SortKey.orderByDouble(0, 1, 0, max + 1));
    SortKey.orderByDouble(0, 1, 0, belowHalfMax - 1)
      .thenByDouble(0, 1, 0, belowHalfMax - 1);
    assertThrows(IllegalArgumentException.class, () -> SortKey.orderByDouble(0, 1, 0, aboveHalfMax)
      .thenByDouble(0, 1, 0, aboveHalfMax));
  }
}
