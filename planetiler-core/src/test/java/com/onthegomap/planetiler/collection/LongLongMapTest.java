package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class LongLongMapTest {

  protected LongLongMap.SequentialWrites map;

  @Test
  public void missingValue() {
    assertEquals(Long.MIN_VALUE, map.get(0));
  }

  @Test
  public void insertLookup() {
    map.put(1, 1);
    assertEquals(Long.MIN_VALUE, map.get(0));
    assertEquals(1, map.get(1));
    assertEquals(Long.MIN_VALUE, map.get(2));
  }

  @Test
  public void insertWithGaps() {
    map.put(1, 2);
    map.put(50, 3);
    map.put(500, 4);
    map.put(505, 5);
    assertEquals(Long.MIN_VALUE, map.get(0));
    assertEquals(2, map.get(1));
    assertEquals(Long.MIN_VALUE, map.get(2));
    assertEquals(Long.MIN_VALUE, map.get(49));
    assertEquals(3, map.get(50));
    assertEquals(Long.MIN_VALUE, map.get(51));
    assertEquals(Long.MIN_VALUE, map.get(300));
    assertEquals(Long.MIN_VALUE, map.get(499));
    assertEquals(4, map.get(500));
    assertEquals(Long.MIN_VALUE, map.get(501));
    assertEquals(5, map.get(505));
    assertEquals(Long.MIN_VALUE, map.get(506));
    assertEquals(Long.MIN_VALUE, map.get(1_000));
  }

  @Test
  public void insertMultiLookup() {
    map.put(1, 3);
    map.put(2, 4);
    map.put(1_000_000_000, Long.MAX_VALUE);
    assertEquals(Long.MIN_VALUE, map.get(0));
    assertEquals(Long.MIN_VALUE, map.get(3));
    assertArrayEquals(new long[]{3, 4, Long.MAX_VALUE, Long.MIN_VALUE},
      map.multiGet(new long[]{1, 2, 1_000_000_000, 3}));
  }

  @Test
  public void bigMultiInsert() {
    long[] key = new long[50000];
    long[] expected = new long[50000];
    for (int i = 0; i < 50000; i++) {
      map.put(i * 4, i + 1);
      key[i] = i * 4;
      expected[i] = i + 1;
    }

    long[] result = map.multiGet(key);

    assertArrayEquals(expected, result);
  }

  public static class SortedTable extends LongLongMapTest {

    @BeforeEach
    public void setup() {
      this.map = new LongLongMap.SortedTable(
        new AppendStore.SmallLongs(
          i -> new AppendStoreRam.Ints()
        ),
        new AppendStoreRam.Longs()
      );
    }
  }

  public static class SparseArray3 extends LongLongMapTest {

    @BeforeEach
    public void setup() {
      this.map = new LongLongMap.SparseArray(new AppendStoreRam.Longs());
    }
  }

  public static class Direct extends LongLongMapTest {

    @BeforeEach
    public void setup() {
      this.map = new LongLongMap.SparseArray(new AppendStoreDirect.Longs());
    }
  }

  public static class ArrayMmap extends LongLongMapTest {

    @BeforeEach
    public void setup(@TempDir Path path) {
      var underlying = new ArrayLongLongMapMmap(path.resolve("node.db"));
      var writer = underlying.newWriter();
      this.map = new LongLongMap.SequentialWrites() {
        @Override
        public void put(long key, long value) {
          writer.put(key, value);
        }

        @Override
        public long get(long key) {
          return underlying.get(key);
        }

        @Override
        public void close() throws IOException {
          writer.close();
          underlying.close();
        }
      };
    }
  }
}
