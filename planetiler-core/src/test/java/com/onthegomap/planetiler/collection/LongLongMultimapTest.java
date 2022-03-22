package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.carrotsearch.hppc.LongArrayList;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class LongLongMultimapTest {

  protected LongLongMultimap map;
  protected boolean retainInputOrder = false;

  @Test
  public void missingValue() {
    assertTrue(map.get(0).isEmpty());
  }

  @Test
  public void oneValue() {
    put(1, 1);
    assertResultLists(LongArrayList.from(), map.get(0));
    assertResultLists(LongArrayList.from(1), map.get(1));
    assertResultLists(LongArrayList.from(), map.get(2));
  }

  private void put(int k, int v) {
    if (map instanceof LongLongMultimap.Replaceable r) {
      r.replaceValues(k, LongArrayList.from(v));
    } else if (map instanceof LongLongMultimap.Appendable a) {
      a.put(k, v);
    } else {
      throw new UnsupportedOperationException(map.getClass().getCanonicalName());
    }
  }

  private void putAll(long k, LongArrayList vs) {
    if (map instanceof LongLongMultimap.Replaceable r) {
      r.replaceValues(k, vs);
    } else if (map instanceof LongLongMultimap.Appendable a) {
      a.putAll(k, vs);
    } else {
      throw new UnsupportedOperationException(map.getClass().getCanonicalName());
    }
  }

  @Test
  public void twoConsecutiveValues() {
    put(1, 1);
    put(2, 2);
    assertResultLists(LongArrayList.from(), map.get(0));
    assertResultLists(LongArrayList.from(1), map.get(1));
    assertResultLists(LongArrayList.from(2), map.get(2));
    assertResultLists(LongArrayList.from(), map.get(3));
  }

  @Test
  public void twoNonconsecutiveValues() {
    put(1, 1);
    put(3, 3);
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
    put(3, 31);
    put(2, 21);
    put(1, 11);
    put(1, 12);
    put(2, 22);
    put(3, 32);
    put(3, 33);
    put(2, 23);
    put(1, 13);
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
      putAll(i, LongArrayList.from(toInsert));
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
      putAll(i, LongArrayList.from(
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
    putAll(1, LongArrayList.from(1, 2, 3));
    put(0, 3);
    assertResultLists(LongArrayList.from(3), map.get(0));
    assertResultLists(LongArrayList.from(1, 2, 3), map.get(1));
    assertResultLists(LongArrayList.from(), map.get(2));
  }

  public static class SparseUnorderedTest extends LongLongMultimapTest {

    @BeforeEach
    public void setup() {
      this.map = LongLongMultimap.newAppendableMultimap();
    }
  }

  public static class DenseOrderedTest extends LongLongMultimapTest {

    @BeforeEach
    public void setup() {
      retainInputOrder = true;
      this.map =
        LongLongMultimap.newInMemoryReplaceableMultimap();
    }
  }

  public static class DenseOrderedMmapTest extends LongLongMultimapTest {

    @BeforeEach
    public void setup(@TempDir Path dir) {
      retainInputOrder = true;
      this.map =
        LongLongMultimap.newReplaceableMultimap(Storage.MMAP, new Storage.Params(dir.resolve("multimap"), true));
    }

    @AfterEach
    public void teardown() {
      this.map.close();
    }
  }
}
