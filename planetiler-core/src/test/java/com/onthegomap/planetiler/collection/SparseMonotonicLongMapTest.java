package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SparseMonotonicLongMapTest {

  private final SparseMonotonicLongMap map = new SparseMonotonicLongMap();

  @Test
  void testEmpty() {
    assertEquals(-1, map.get(0));
    assertEquals(-1, map.get(1));
    assertEquals(-1, map.get(1_000_000));
    assertEquals(-1, map.get(-1));
  }

  @Test
  void testSingleKey() {
    map.add(5, 10);
    assertEquals(10, map.get(5));
    assertEquals(-1, map.next(5));
    assertEquals(-1, map.get(4));
    assertEquals(-1, map.get(6));
    assertEquals(-1, map.get(512 + 5));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1, 63, 64, 65, 127, 128, 510, 511, 512, 513, 1023, 1024, 100_000})
  void testKeysAroundBitmapWordsAndPages(long key) {
    map.add(0, 0);
    if (key > 0) {
      map.add(key, 100);
    }
    map.add(key + 1, 200);
    map.add(key + 1_000, 300);
    if (key > 0) {
      assertEquals(0, map.get(0));
      assertEquals(100, map.next(0));
    }
    if (key > 1) {
      assertEquals(-1, map.get(key - 1));
    }
    assertEquals(key > 0 ? 100 : 0, map.get(key));
    assertEquals(200, map.next(key));
    assertEquals(200, map.get(key + 1));
    assertEquals(300, map.next(key + 1));
    assertEquals(-1, map.get(key + 2));
    assertEquals(-1, map.get(key + 999));
    assertEquals(300, map.get(key + 1_000));
    assertEquals(-1, map.next(key + 1_000));
    assertEquals(-1, map.get(key + 1_001));
  }

  @Test
  void testFullPages() {
    for (int i = 0; i < 2048; i++) {
      map.add(i, i * 256L);
    }
    for (int i = 0; i < 2048; i++) {
      assertEquals(i * 256L, map.get(i));
      assertEquals(i == 2047 ? -1 : (i + 1) * 256L, map.next(i));
    }
    assertEquals(-1, map.get(2048));
  }

  @Test
  void testEveryOtherKey() {
    for (int i = 0; i < 2048; i += 2) {
      map.add(i, i * 10L);
    }
    for (int i = 0; i < 2048; i++) {
      assertEquals(i % 2 == 0 ? i * 10L : -1, map.get(i));
    }
    for (int i = 0; i < 2046; i += 2) {
      assertEquals((i + 2) * 10L, map.next(i));
    }
  }

  @Test
  void testPageEndsWhereNextPageStartsAfterManyEmptyPages() {
    map.add(511, 1_000_000);
    map.add(50_000_000L, 1_000_100);
    assertEquals(1_000_000, map.get(511));
    assertEquals(1_000_100, map.next(511));
    assertEquals(1_000_100, map.get(50_000_000L));
    assertEquals(-1, map.next(50_000_000L));
    assertEquals(-1, map.get(512));
    assertEquals(-1, map.get(49_999_999L));
  }

  @Test
  void testLargeValues() {
    long base = 1L << 50;
    map.add(1, base);
    map.add(2, base + Integer.MAX_VALUE);
    assertEquals(base, map.get(1));
    assertEquals(base + Integer.MAX_VALUE, map.get(2));
  }

  @Test
  void testRejectsOutOfOrderKeys() {
    map.add(5, 10);
    assertThrows(IllegalArgumentException.class, () -> map.add(5, 20));
    assertThrows(IllegalArgumentException.class, () -> map.add(4, 20));
  }

  @Test
  void testMemoryScalesWithKeysAddedNotLargestKey() {
    // node maps add one key per 256 node IDs, up to the largest node ID today (~14 billion)
    map.add(1, 0);
    map.add(14_000_000_000L / 256, 1);
    long bytes = map.estimateMemoryUsageBytes();
    // a dense key of 4 bytes per key would need ~220MB, the page pointers alone ~850kB
    assertTrue(bytes < 1_000_000, "estimated " + bytes + " bytes");
  }

  @Test
  void testNextRejectsKeysNotAdded() {
    map.add(5, 10);
    assertThrows(IllegalArgumentException.class, () -> map.next(4));
    assertThrows(IllegalArgumentException.class, () -> map.next(6));
    assertThrows(IllegalArgumentException.class, () -> map.next(100_000));
  }
}
