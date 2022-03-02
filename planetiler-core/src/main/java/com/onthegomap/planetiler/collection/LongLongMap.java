package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;

import com.carrotsearch.hppc.ByteArrayList;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * A map that stores a single {@code long} value for each OSM node. A single thread writes the values for each node ID
 * sequentially then multiple threads can read values concurrently.
 * <p>
 * Three implementations are provided: {@link #noop()} which ignores writes and throws on reads, {@link SortedTable}
 * which stores node IDs and values sorted by node ID and does binary search on lookup, and {@link SparseArray} which
 * only stores values and uses the node ID as the index into the array (with some compression to avoid storing many
 * sequential 0's).
 * <p>
 * Use {@link SortedTable} for small OSM extracts and {@link SparseArray} when processing the entire planet.
 * <p>
 * Each implementation can be backed by either {@link AppendStoreRam} to store data in RAM or {@link AppendStoreMmap} to
 * store data in a memory-mapped file.
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
   * @param name    which implementation to use: {@code "noop"}, {@code "sortedtable"} or {@code "sparsearray"}
   * @param storage how to store data: {@code "ram"}, {@code "mmap"}, or {@code "direct"}
   * @param path    where to store data (if mmap)
   * @return A longlong map instance
   * @throws IllegalArgumentException if {@code name} or {@code storage} is not valid
   */
  static LongLongMap from(String name, String storage, Path path) {
    // TODO turn these storage and long long map types into enums
    if ("array".equals(name)) {
      FileUtils.createDirectory(path);
      return new ArrayLongLongMapMmap(path.resolve("nodes"));
    }

    boolean ram = switch (storage) {
      case "ram", "direct" -> true;
      case "mmap" -> false;
      default -> throw new IllegalArgumentException("Unexpected storage value: " + storage);
    };
    boolean direct = "direct".equals(storage);

    return switch (name) {
      case "noop" -> noop();
      case "sortedtable" -> ram ? (direct ? newDirectSortedTable() : newInMemorySortedTable())
        : newDiskBackedSortedTable(path);
      case "sparsearray" -> ram ? (direct ? newDirectSparseArray() : newInMemorySparseArray())
        : newDiskBackedSparseArray(path);
      default -> throw new IllegalArgumentException("Unexpected value: " + name);
    };
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
      public void close() {
      }
    };
  }

  /** Returns an in-memory longlong map that uses 12-bytes per node and binary search to find values. */
  static LongLongMap newInMemorySortedTable() {
    return new SortedTable(
      new AppendStore.SmallLongs(i -> new AppendStoreRam.Ints()),
      new AppendStoreRam.Longs()
    );
  }

  /**
   * Returns a longlong map stored in off-heap (direct) memory that uses 12-bytes per node and binary search to find
   * values.
   */
  static LongLongMap newDirectSortedTable() {
    return new SortedTable(
      new AppendStore.SmallLongs(i -> new AppendStoreDirect.Ints()),
      new AppendStoreDirect.Longs()
    );
  }

  /** Returns a memory-mapped longlong map that uses 12-bytes per node and binary search to find values. */
  static LongLongMap newDiskBackedSortedTable(Path dir) {
    FileUtils.createDirectory(dir);
    return new SortedTable(
      new AppendStore.SmallLongs(i -> new AppendStoreMmap.Ints(dir.resolve("keys-" + i))),
      new AppendStoreMmap.Longs(dir.resolve("values"))
    );
  }

  /**
   * Returns an in-memory longlong map that uses 8-bytes per node and O(1) lookup but wastes space storing lots of 0's
   * when the key space is fragmented.
   */
  static LongLongMap newInMemorySparseArray() {
    return new SparseArray(new AppendStoreRam.Longs());
  }

  /**
   * Returns a longlong map stored off-heap in direct byte buffers that uses 8-bytes per node and O(1) lookup but wastes
   * space storing lots of 0's when the key space is fragmented.
   */
  static LongLongMap newDirectSparseArray() {
    return new SparseArray(new AppendStoreDirect.Longs());
  }

  /**
   * Returns a memory-mapped longlong map that uses 8-bytes per node and O(1) lookup but wastes space storing lots of
   * 0's when the key space is fragmented.
   */
  static LongLongMap newDiskBackedSparseArray(Path path) {
    return new SparseArray(new AppendStoreMmap.Longs(path));
  }

  Writer newWriter();

  /**
   * Returns the value for a key. Safe to be called by multiple threads after all values have been written. After the
   * first read, all writes will fail.
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

  interface Writer extends AutoCloseable {

    /**
     * Writes the value for a key. Not thread safe! All writes must come from a single thread, in order by key. No
     * writes can be performed after the first read.
     */
    void put(long key, long value);

    @Override
    default void close() {
    }
  }

  interface SequentialWrites extends LongLongMap {

    void put(long key, long value);

    @Override
    default Writer newWriter() {
      return this::put;
    }
  }

  interface ParallelWrites extends LongLongMap {
  }

  /**
   * A longlong map that stores keys and values sorted by key and does a binary search to lookup values.
   */
  class SortedTable implements LongLongMap, SequentialWrites {

    /*
     * It's not actually a binary search, it keeps track of the first index of each block of 256 keys, so it
     * can do an O(1) lookup to narrow down the search space to 256 values.
     */
    private final AppendStore.Longs offsets = new AppendStoreRam.Longs();
    private final AppendStore.Longs keys;
    private final AppendStore.Longs values;
    private long lastChunk = -1;
    private long lastKey = -1;

    public SortedTable(AppendStore.Longs keys, AppendStore.Longs values) {
      this.keys = keys;
      this.values = values;
    }

    @Override
    public void put(long key, long value) {
      if (key <= lastKey) {
        throw new IllegalArgumentException("Nodes must be sorted ascending by ID, " + key + " came after " + lastKey);
      }
      lastKey = key;
      long idx = keys.size();
      long chunk = key >>> 8;
      if (chunk != lastChunk) {
        while (offsets.size() <= chunk) {
          offsets.appendLong(idx);
        }
        lastChunk = chunk;
      }
      keys.appendLong(key);
      values.appendLong(value);
    }

    @Override
    public long get(long key) {
      long chunk = key >>> 8;
      if (chunk >= offsets.size()) {
        return MISSING_VALUE;
      }

      // use the "offsets" index to narrow search space to <256 values
      long lo = offsets.getLong(chunk);
      long hi = Math.min(keys.size(), chunk >= offsets.size() - 1 ? keys.size() : offsets.getLong(chunk + 1)) - 1;

      while (lo <= hi) {
        long idx = (lo + hi) >>> 1;
        long value = keys.getLong(idx);
        if (value < key) {
          lo = idx + 1;
        } else if (value > key) {
          hi = idx - 1;
        } else {
          // found
          return values.getLong(idx);
        }
      }
      return MISSING_VALUE;
    }

    @Override
    public long diskUsageBytes() {
      return keys.diskUsageBytes() + values.diskUsageBytes();
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return keys.estimateMemoryUsageBytes() + values.estimateMemoryUsageBytes() + offsets.estimateMemoryUsageBytes();
    }

    @Override
    public void close() throws IOException {
      keys.close();
      values.close();
      offsets.close();
    }
  }

  /**
   * A longlong map that only stores values and uses the key as an index into the array, with some tweaks to avoid
   * storing many sequential 0's.
   */
  class SparseArray implements LongLongMap, SequentialWrites {

    // The key space is broken into chunks of 256 and for each chunk, store:
    // 1) the index in the outputs array for the first key in the block
    private final AppendStore.Longs offsets = new AppendStoreRam.Longs();
    // 2) the number of leading 0's at the start of each block
    private final ByteArrayList offsetStartPad = new ByteArrayList();

    private final AppendStore.Longs values;
    private int lastChunk = -1;
    private int lastOffset = 0;
    private long lastKey = -1;

    public SparseArray(AppendStore.Longs values) {
      this.values = values;
    }

    @Override
    public void put(long key, long value) {
      if (key <= lastKey) {
        throw new IllegalArgumentException("Nodes must be sorted ascending by ID, " + key + " came after " + lastKey);
      }
      lastKey = key;
      long idx = values.size();
      int chunk = (int) (key >>> 8);
      int offset = (int) (key & 255);

      if (chunk != lastChunk) {
        // new chunk, store offset and leading zeros
        lastOffset = offset;
        while (offsets.size() <= chunk) {
          offsets.appendLong(idx);
          offsetStartPad.add((byte) offset);
        }
        lastChunk = chunk;
      } else {
        // same chunk, write not_founds until we get to right idx
        while (++lastOffset < offset) {
          values.appendLong(MISSING_VALUE);
        }
      }
      values.appendLong(value);
    }

    @Override
    public long get(long key) {
      int chunk = (int) (key >>> 8);
      int offset = (int) (key & 255);
      if (chunk >= offsets.size()) {
        return MISSING_VALUE;
      }

      long lo = offsets.getLong(chunk);
      long hi = Math.min(values.size(), chunk >= offsets.size() - 1 ? values.size() : offsets.getLong(chunk + 1)) - 1;
      int startPad = offsetStartPad.get(chunk) & 255;

      long index = lo + offset - startPad;

      if (index > hi || index < lo) {
        return MISSING_VALUE;
      }

      return values.getLong(index);
    }

    @Override
    public long diskUsageBytes() {
      return values.diskUsageBytes();
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return values.estimateMemoryUsageBytes() + estimateSize(offsets) + estimateSize(offsetStartPad);
    }

    @Override
    public void close() throws IOException {
      offsetStartPad.release();
      values.close();
      offsets.close();
    }
  }
}
