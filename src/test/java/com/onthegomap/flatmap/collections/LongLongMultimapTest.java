package com.onthegomap.flatmap.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.carrotsearch.hppc.LongArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class LongLongMultimapTest {

  protected LongLongMultimap map;
  protected boolean retainInputOrder = false;

  @Test
  public void missingValue() {
    assertTrue(map.get(0).isEmpty());
  }

  @Test
  public void oneValue() {
    map.put(1, 1);
    assertResultLists(LongArrayList.from(), map.get(0));
    assertResultLists(LongArrayList.from(1), map.get(1));
    assertResultLists(LongArrayList.from(), map.get(2));
  }

  @Test
  public void twoConsecutiveValues() {
    map.put(1, 1);
    map.put(2, 2);
    assertResultLists(LongArrayList.from(), map.get(0));
    assertResultLists(LongArrayList.from(1), map.get(1));
    assertResultLists(LongArrayList.from(2), map.get(2));
    assertResultLists(LongArrayList.from(), map.get(3));
  }

  @Test
  public void twoNonconsecutiveValues() {
    map.put(1, 1);
    map.put(3, 3);
    assertResultLists(LongArrayList.from(), map.get(0));
    assertResultLists(LongArrayList.from(1), map.get(1));
    assertResultLists(LongArrayList.from(), map.get(2));
    assertResultLists(LongArrayList.from(3), map.get(3));
    assertResultLists(LongArrayList.from(), map.get(4));
  }

  @Test
  public void returnToFirstKey() {
    if (retainInputOrder) {
      return;
    }
    map.put(3, 31);
    map.put(2, 21);
    map.put(1, 11);
    map.put(1, 12);
    map.put(2, 22);
    map.put(3, 32);
    map.put(3, 33);
    map.put(2, 23);
    map.put(1, 13);
    assertResultLists(LongArrayList.from(11, 12, 13), map.get(1));
    assertResultLists(LongArrayList.from(21, 22, 23), map.get(2));
    assertResultLists(LongArrayList.from(31, 32, 33), map.get(3));
    assertResultLists(LongArrayList.from(), map.get(4));
  }

  @Test
  public void manyInsertsOrdered() {
    long[] toInsert = new long[10];
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 10; j++) {
        toInsert[j] = i * 10 + j + 1;
      }
      map.putAll(i, LongArrayList.from(toInsert));
    }
    for (int i = 0; i < 100; i++) {
      assertResultLists(LongArrayList.from(
        i * 10 + 1,
        i * 10 + 2,
        i * 10 + 3,
        i * 10 + 4,
        i * 10 + 5,
        i * 10 + 6,
        i * 10 + 7,
        i * 10 + 8,
        i * 10 + 9,
        i * 10 + 10
      ), map.get(i));
    }
  }

  private void assertResultLists(LongArrayList expected, LongArrayList actual) {
    if (!retainInputOrder) {
      if (!expected.isEmpty()) {
        Arrays.sort(expected.buffer, 0, expected.size());
      }
      if (!actual.isEmpty()) {
        Arrays.sort(actual.buffer, 0, actual.size());
      }
    }
    assertEquals(expected, actual);
  }

  @Test
  public void manyInsertsUnordered() {
    for (long i = 99; i >= 0; i--) {
      map.putAll(i, LongArrayList.from(
        i * 10 + 10,
        i * 10 + 9,
        i * 10 + 8,
        i * 10 + 7,
        i * 10 + 6,
        i * 10 + 5,
        i * 10 + 4,
        i * 10 + 3,
        i * 10 + 2,
        i * 10 + 1
      ));
    }
    for (int i = 0; i < 100; i++) {
      assertResultLists(LongArrayList.from(
        i * 10 + 10,
        i * 10 + 9,
        i * 10 + 8,
        i * 10 + 7,
        i * 10 + 6,
        i * 10 + 5,
        i * 10 + 4,
        i * 10 + 3,
        i * 10 + 2,
        i * 10 + 1
      ), map.get(i));
    }
  }

  @Test
  public void multiInsert() {
    map.putAll(1, LongArrayList.from(1, 2, 3));
    map.put(0, 3);
    assertResultLists(LongArrayList.from(3), map.get(0));
    assertResultLists(LongArrayList.from(1, 2, 3), map.get(1));
    assertResultLists(LongArrayList.from(), map.get(2));
  }

  public static class SparseUnorderedTest extends LongLongMultimapTest {

    @BeforeEach
    public void setup() {
      this.map = LongLongMultimap.newSparseUnorderedMultimap();
    }
  }

  public static class DenseOrderedTest extends LongLongMultimapTest {

    @BeforeEach
    public void setup() {
      retainInputOrder = true;
      this.map = LongLongMultimap.newDensedOrderedMultimap();
    }
  }
}
