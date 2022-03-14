package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;

import com.carrotsearch.hppc.ByteArrayList;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * A map that stores a single {@code long} value for each OSM node. A single thread writes the values for each node ID
 * sequentially then multiple threads can read values concurrently.
 * <p>
 * See {@link Type} for the available implementations.
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
      case SPARSE_ARRAY -> new SparseArray(AppendStore.Longs.create(storage, params));
      case SORTED_TABLE -> new SortedTable(
        new AppendStore.SmallLongs(i -> AppendStore.Ints.create(storage, params.resolve("keys-" + i))),
        AppendStore.Longs.create(storage, params.resolve("values"))
      );
      case ARRAY -> switch (storage) {
          case MMAP -> new ArrayLongLongMapMmap(params.path(), params.madvise());
          case RAM -> new ArrayLongLongMapRam();
          case DIRECT -> new ArrayLongLongMapDirect();
        };
    };
  }

  static LongLongMap newInMemorySortedTable() {
    return from(Type.SORTED_TABLE, Storage.RAM, new Storage.Params(Path.of("."), false));
  }

  record StorageRequired(long onHeapBytes, long offHeapBytes, long diskBytes) {

    static StorageRequired ZERO = new StorageRequired(0, 0, 0);
    static StorageRequired fixed(Storage type, long bytes) {
      return ZERO.plus(type, bytes);
    }

    StorageRequired plus(Storage type, long bytes) {
      return new StorageRequired(
        onHeapBytes + (type == Storage.RAM ? bytes : 0),
        offHeapBytes + (type == Storage.DIRECT ? bytes : 0),
        diskBytes + (type == Storage.MMAP ? bytes : 0)
      );
    }
  }

  /** Estimates the resource requirements for this nodemap for a given OSM input file. */
  static StorageRequired estimateStorageRequired(String name, String storage, long osmFileSize) {
    return estimateStorageRequired(Type.from(name), Storage.from(storage), osmFileSize);
  }

  /** Estimates the resource requirements for this nodemap for a given OSM input file. */
  static StorageRequired estimateStorageRequired(Type type, Storage storage, long osmFileSize) {
    long nodes = estimateNumNodes(osmFileSize);
    long maxNodeId = estimateMaxNodeId(osmFileSize);

    return switch (type) {
      case NOOP -> StorageRequired.ZERO;
      case SPARSE_ARRAY -> StorageRequired.fixed(Storage.RAM, 300_000_000L).plus(storage, 9 * nodes);
      case SORTED_TABLE -> StorageRequired.fixed(Storage.RAM, 300_000_000L).plus(storage, 12 * nodes);
      case ARRAY -> StorageRequired.fixed(storage, 8 * maxNodeId)
        // memory-mapped array storage uses byte buffers for temporary storage
        .plus(Storage.RAM, storage == Storage.MMAP ? ArrayLongLongMapMmap.estimateTempMemoryUsageBytes() : 0);
    };
  }

  private static long estimateNumNodes(long osmFileSize) {
    // On 2/14/2022, planet.pbf was 66691979646 bytes with ~750m nodes, so scale from there
    return Math.round(750_000_000d * (osmFileSize / 66_691_979_646d));
  }

  private static long estimateMaxNodeId(long osmFileSize) {
    // On 2/14/2022, planet.pbf was 66691979646 bytes and max node ID was ~9.5b, so scale from there
    // but don't go less than 9.5b in case it's an extract
    return Math.round(9_500_000_000d * Math.max(1, osmFileSize / 66_691_979_646d));
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

  /** Which long long map implementation to use. */
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

    private final String name;

    Type(String name) {
      this.name = name;
    }

    /**
     * Returns the type associated with {@code name} or throws {@link IllegalArgumentException} if no match is found.
     */
    public static Type from(String name) {
      for (Type value : values()) {
        if (value.name.equalsIgnoreCase(name.trim())) {
          return value;
        }
      }
      throw new IllegalArgumentException("Unexpected long long map type: " + name);
    }
  }

  interface Writer extends AutoCloseable {

    /**
     * Writes the value for a key. Not thread safe! All writes must come from a single thread, in order by key. No
     * writes can be performed after the first read.
     */
    void put(long key, long value);

    @Override
    default void close() {}
  }

  interface SequentialWrites extends LongLongMap {

    void put(long key, long value);

    @Override
    default Writer newWriter() {
      return this::put;
    }
  }

  interface ParallelWrites extends LongLongMap {}

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
