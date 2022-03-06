package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AppendStoreTest {

  abstract static class IntsTest {

    protected AppendStore.Ints store;

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    public void writeThenRead(int num) {
      for (int i = 0; i < num; i++) {
        store.appendInt(i + 1);
      }
      for (int i = 0; i < num; i++) {
        assertEquals(i + 1, store.getInt(i));
      }
      assertThrows(IndexOutOfBoundsException.class, () -> store.getInt(num));
      assertThrows(IndexOutOfBoundsException.class, () -> store.getInt(num + 1));
    }

    @Test
    public void readBig() {
      store.appendInt(Integer.MAX_VALUE);
      store.appendInt(Integer.MAX_VALUE - 1);
      store.appendInt(Integer.MAX_VALUE - 2);
      assertEquals(Integer.MAX_VALUE, store.getInt(0));
      assertEquals(Integer.MAX_VALUE - 1, store.getInt(1));
      assertEquals(Integer.MAX_VALUE - 2, store.getInt(2));
    }
  }

  abstract static class LongsTest {

    protected AppendStore.Longs store;

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    public void writeThenRead(int num) {
      for (int i = 0; i < num; i++) {
        store.appendLong(i + 1);
      }
      for (int i = 0; i < num; i++) {
        assertEquals(i + 1, store.getLong(i));
      }
      assertThrows(IndexOutOfBoundsException.class, () -> store.getLong(num));
      assertThrows(IndexOutOfBoundsException.class, () -> store.getLong(num + 1));
    }

    private static final long maxInt = Integer.MAX_VALUE;

    @ParameterizedTest
    @ValueSource(
        longs = {
          maxInt - 1,
          maxInt,
          maxInt + 1,
          2 * maxInt - 1,
          2 * maxInt,
          5 * maxInt - 1,
          5 * maxInt + 1
        })
    public void readBig(long value) {
      store.appendLong(value);
      assertEquals(value, store.getLong(0));
    }
  }

  static class RamInt extends IntsTest {

    @BeforeEach
    public void setup() {
      this.store = new AppendStoreRam.Ints(4 << 2);
    }
  }

  static class MMapInt extends IntsTest {

    @BeforeEach
    public void setup(@TempDir Path path) {
      this.store = new AppendStoreMmap.Ints(path.resolve("ints"), 4 << 2);
    }
  }

  static class RamLong extends LongsTest {

    @BeforeEach
    public void setup() {
      this.store = new AppendStoreRam.Longs(4 << 2);
    }
  }

  static class MMapLong extends LongsTest {

    @BeforeEach
    public void setup(@TempDir Path path) {
      this.store = new AppendStoreMmap.Longs(path.resolve("longs"), 4 << 2);
    }
  }

  static class MMapSmallLong extends LongsTest {

    @BeforeEach
    public void setup(@TempDir Path path) {
      this.store =
          new AppendStore.SmallLongs(
              (i) -> new AppendStoreMmap.Ints(path.resolve("smalllongs" + i), 4 << 2));
    }
  }

  static class RamSmallLong extends LongsTest {

    @BeforeEach
    public void setup() {
      this.store = new AppendStore.SmallLongs((i) -> new AppendStoreRam.Ints(4 << 2));
    }
  }
}
