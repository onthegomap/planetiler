package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.Closeable;
import java.nio.file.Path;

/**
 * A map that stores a single {@code long} value for each OSM node.
 * <p>
 * See {@link Type} for the available map implementations and {@link Storage} for the available storage implementations.
 */
public interface LongLongMap extends Closeable, MemoryEstimator.HasEstimate, DiskBacked {
  /*
   * Idea graveyard (all too slow):
   * - rocksdb
   * - mapdb sorted table
   * - sqlite table with key and value columns
   */

  long MISSING_VALUE = Long.MIN_VALUE;

  /**
   * Returns a new longlong map from config strings.
   *
   * @param name    name of the {@link Type} implementation to use
   * @param storage name of the {@link Storage} implementation to use
   * @param path    where to store data (if mmap)
   * @param madvise whether to use linux madvise random to improve read performance
   * @return A longlong map instance
   * @throws IllegalArgumentException if {@code name} or {@code storage} is not valid
   */
  static LongLongMap from(String name, String storage, Path path, boolean madvise) {
    return from(Type.from(name), Storage.from(storage), new Storage.Params(path, madvise));
  }

  /**
   * Returns a new longlong map.
   *
   * @param type    The {@link Type} implementation to use
   * @param storage The {@link Storage} implementation to use
   * @param params  Parameters to pass to storage layer
   * @return A longlong map instance
   */
  static LongLongMap from(Type type, Storage storage, Storage.Params params) {
    return switch (type) {
      case NOOP -> noop();
      case SPARSE_ARRAY -> new SparseArrayLongLongMap(AppendStore.Longs.create(storage, params));
      case SORTED_TABLE -> new SortedTableLongLongMap(
        new AppendStore.SmallLongs(i -> AppendStore.Ints.create(storage, params.resolve("keys-" + i))),
        AppendStore.Longs.create(storage, params.resolve("values"))
      );
      case ARRAY -> switch (storage) {
          case MMAP -> new ArrayLongLongMapMmap(params.path(), params.madvise());
          case RAM -> new ArrayLongLongMapRam(false);
          case DIRECT -> new ArrayLongLongMapRam(true);
        };
    };
  }

  /** Returns a new long map using {@link Type#SORTED_TABLE} and {@link Storage#RAM}. */
  static LongLongMap newInMemorySortedTable() {
    return from(Type.SORTED_TABLE, Storage.RAM, new Storage.Params(Path.of("."), false));
  }

  /** Returns a longlong map that stores no data and throws on read */
  static LongLongMap noop() {
    return new ParallelWrites() {
      @Override
      public Writer newWriter() {
        return (key, value) -> {
        };
      }

      @Override
      public long get(long key) {
        throw new UnsupportedOperationException("get");
      }

      @Override
      public long diskUsageBytes() {
        return 0;
      }

      @Override
      public void close() {}
    };
  }

  /** Returns a {@link Writer} that a single thread can use to do writes into this map. */
  Writer newWriter();

  /**
   * Returns the value for {@code key}. Safe to be called by multiple threads after all values have been written. After
   * the first read, all writes will fail.
   */
  long get(long key);

  @Override
  default long diskUsageBytes() {
    return 0;
  }

  @Override
  default long estimateMemoryUsageBytes() {
    return 0;
  }

  default long[] multiGet(long[] key) {
    long[] result = new long[key.length];
    for (int i = 0; i < key.length; i++) {
      result[i] = get(key[i]);
    }
    return result;
  }

  /** Which long map implementation to use. */
  enum Type {
    /** Ignore writes and throw an exception on reads. */
    NOOP("noop"),

    /**
     * Store an ordered list of keys, and an ordered list of values, and on read do a binary search on keys to find the
     * index of the value to read.
     * <p>
     * Uses exactly 12 bytes per value stored so is ideal for small extracts.
     * <p>
     * NOTE: Requires ordered writes from a single thread.
     */
    SORTED_TABLE("sortedtable"),

    /**
     * Stores values in many small arrays indexed by key, compressing large ranges from the key space.
     * <p>
     * Uses around ~9 bytes per value stored as the input approaches full planet size. Ideal for full-planet imports
     * when you want to use as little memory as possible.
     * <p>
     * NOTE: Requires ordered writes from a single thread.
     */
    SPARSE_ARRAY("sparsearray"),

    /**
     * Stores values in indexed by key, without compressing unused ranges from the key space so that writes can be done
     * from multiple threads in parallel.
     * <p>
     * Uses exactly {@code maxNodeId * 8} bytes. Suitable only for full-planet imports that use {@link Storage#MMAP}
     * storage or have plenty of extra RAM.
     */
    ARRAY("array");

    private final String id;

    Type(String id) {
      this.id = id;
    }

    /**
     * Returns the type associated with {@code id} or throws {@link IllegalArgumentException} if no match is found.
     */
    public static Type from(String id) {
      for (Type value : values()) {
        if (value.id.equalsIgnoreCase(id.trim())) {
          return value;
        }
      }
      throw new IllegalArgumentException("Unexpected long long map type: " + id);
    }

    public String id() {
      return id;
    }
  }

  /** A handle for a single thread to use to insert into this map. */
  interface Writer extends AutoCloseable {

    /**
     * Writes the value for a key. Not thread safe! All calls to this method must come from a single thread, in order by
     * key. No writes can be performed after the first read.
     */
    void put(long key, long value);

    @Override
    default void close() {}
  }

  /** Implementations that only support sequential writes from a single thread. */
  interface SequentialWrites extends LongLongMap {

    void put(long key, long value);

    @Override
    default Writer newWriter() {
      return this::put;
    }
  }

  /** Implementations that support parallel writes from multiple threads. */
  interface ParallelWrites extends LongLongMap {}

}
