package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.Memoized.memoize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.ExpectedException;
import org.junit.jupiter.api.Test;

class MemoizedTest {
  int calls = 0;

  @Test
  void testMemoize() {
    Memoized<Integer, Integer> memoized = memoize(i -> {
      calls++;
      return i + 1;
    });
    assertEquals(0, calls);
    assertEquals(1, memoized.apply(0));
    assertEquals(1, calls);
    assertEquals(1, memoized.apply(0));
    assertEquals(1, memoized.tryApply(0).get());
    assertEquals(1, calls);
    assertEquals(2, memoized.apply(1));
    assertEquals(2, memoized.apply(1));
    assertEquals(2, calls);
  }

  @Test
  void testThrowException() {
    Memoized<Integer, Integer> memoized = memoize(i -> {
      calls++;
      throw new ExpectedException();
    });
    assertEquals(0, calls);
    assertThrows(ExpectedException.class, () -> memoized.apply(0));
    assertThrows(ExpectedException.class, () -> memoized.apply(0));
    assertTrue(memoized.tryApply(0).isFailure());
    assertEquals(1, calls);
    assertThrows(ExpectedException.class, () -> memoized.apply(1));
    assertThrows(ExpectedException.class, () -> memoized.apply(1));
    assertTrue(memoized.tryApply(1).isFailure());
    assertEquals(2, calls);
  }

  @Test
  void testTryCast() {
    Memoized<Integer, Integer> memoized = memoize(i -> {
      calls++;
      return i + 1;
    });
    assertEquals(1, memoized.tryApply(0, Number.class).get());
    var failed = memoized.tryApply(0, String.class);
    assertTrue(failed.isFailure());
    assertThrows(ClassCastException.class, failed::get);
  }
}
