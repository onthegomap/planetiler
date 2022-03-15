package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class LongLongMapTest {

  private LongLongMap.SequentialWrites sequential;

  protected abstract LongLongMap.SequentialWrites createSequentialWriter(Path path);

  @BeforeEach
  public void setupSequentialWriter(@TempDir Path path) {
    this.sequential = createSequentialWriter(path);
  }

  @Test
  public void missingValue() {
    assertEquals(Long.MIN_VALUE, sequential.get(0));
  }

  @Test
  public void insertLookup() {
    sequential.put(1, 1);
    assertEquals(Long.MIN_VALUE, sequential.get(0));
    assertEquals(1, sequential.get(1));
    assertEquals(Long.MIN_VALUE, sequential.get(2));
  }

  @Test
  public void insertLookupHighValue() {
    sequential.put(1_000_000, 1);
    assertEquals(Long.MIN_VALUE, sequential.get(999_999));
    assertEquals(1, sequential.get(1_000_000));
    assertEquals(Long.MIN_VALUE, sequential.get(1_000_001));
  }

  @Test
  public void insertWithGaps() {
    sequential.put(1, 2);
    sequential.put(50, 3);
    sequential.put(500, 4);
    sequential.put(505, 5);
    assertEquals(Long.MIN_VALUE, sequential.get(0));
    assertEquals(2, sequential.get(1));
    assertEquals(Long.MIN_VALUE, sequential.get(2));
    assertEquals(Long.MIN_VALUE, sequential.get(49));
    assertEquals(3, sequential.get(50));
    assertEquals(Long.MIN_VALUE, sequential.get(51));
    assertEquals(Long.MIN_VALUE, sequential.get(300));
    assertEquals(Long.MIN_VALUE, sequential.get(499));
    assertEquals(4, sequential.get(500));
    assertEquals(Long.MIN_VALUE, sequential.get(501));
    assertEquals(5, sequential.get(505));
    assertEquals(Long.MIN_VALUE, sequential.get(506));
    assertEquals(Long.MIN_VALUE, sequential.get(1_000));
  }

  @Test
  public void insertMultiLookup() {
    sequential.put(1, 3);
    sequential.put(2, 4);
    sequential.put(1_000_000, Long.MAX_VALUE);
    assertEquals(Long.MIN_VALUE, sequential.get(0));
    assertEquals(Long.MIN_VALUE, sequential.get(3));
    assertArrayEquals(new long[]{3, 4, Long.MAX_VALUE, Long.MIN_VALUE},
      sequential.multiGet(new long[]{1, 2, 1_000_000, 3}));
  }

  @Test
  public void bigMultiInsert() {
    long[] key = new long[50000];
    long[] expected = new long[50000];
    for (int i = 0; i < 50000; i++) {
      sequential.put(i * 4, i + 1);
      key[i] = i * 4;
      expected[i] = i + 1;
    }

    long[] result = sequential.multiGet(key);

    assertArrayEquals(expected, result);
  }

  public static class SortedTableTest extends LongLongMapTest {

    @Override
    protected LongLongMap.SequentialWrites createSequentialWriter(Path path) {
      return new SortedTableLongLongMap(
        new AppendStore.SmallLongs(
          i -> new AppendStoreRam.Ints(false)
        ),
        new AppendStoreRam.Longs(false)
      );
    }
  }

  public static class SparseArrayTest extends LongLongMapTest {

    @Override
    protected LongLongMap.SequentialWrites createSequentialWriter(Path path) {
      return new SparseArrayLongLongMap(new AppendStoreRam.Longs(false));
    }
  }

  public static class DirectTest extends LongLongMapTest {

    @Override
    protected LongLongMap.SequentialWrites createSequentialWriter(Path path) {
      return new SparseArrayLongLongMap(new AppendStoreRam.Longs(true));
    }
  }

  public static class AllTest {

    @Test
    public void testAllImplementations(@TempDir Path path) {
      for (Storage storage : Storage.values()) {
        for (LongLongMap.Type type : LongLongMap.Type.values()) {
          var variant = storage + "-" + type;
          var params = new Storage.Params(path.resolve(variant), true);
          LongLongMap map = LongLongMap.from(type, storage, params);
          try (var writer = map.newWriter()) {
            writer.put(2, 3);
            writer.put(4, 5);
          }
          if (type != LongLongMap.Type.NOOP) {
            assertEquals(3, map.get(2), variant);
            assertEquals(5, map.get(4), variant);
          }
        }
      }
    }
  }
}
