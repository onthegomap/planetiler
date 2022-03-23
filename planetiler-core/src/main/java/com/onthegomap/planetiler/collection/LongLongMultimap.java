package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntHashMap;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory map that stores a multiple {@code long} values for each {@code long} key.
 * <p>
 * Implementations extend {@link Replaceable} if they support replacing the previous set of values for a key and/or
 * {@link Appendable} if they support adding new values for a key.
 */
public interface LongLongMultimap extends MemoryEstimator.HasEstimate, DiskBacked, AutoCloseable {

  /** Returns a {@link Noop} implementation that does nothin on put and throws an exception if you try to get. */
  static Noop noop() {
    return new Noop();
  }

  /** Returns a new multimap where each write sets the list of values for a key, and that order is preserved on read. */
  static Replaceable newReplaceableMultimap(Storage storage, Storage.Params params) {
    return new DenseOrderedMultimap(storage, params);
  }

  /** Returns a new replaceable multimap held in-memory. */
  static Replaceable newInMemoryReplaceableMultimap() {
    return newReplaceableMultimap(Storage.RAM, null);
  }

  /** Returns a new multimap where each write adds a value for the given key. */
  static Appendable newAppendableMultimap() {
    return new SparseUnorderedBinarySearchMultimap();
  }

  /**
   * Returns a new longlong multimap from config strings.
   *
   * @param storage name of the {@link Storage} implementation to use
   * @param path    where to store data (if mmap)
   * @param madvise whether to use linux madvise random to improve read performance
   * @return A longlong map instance
   * @throws IllegalArgumentException if {@code name} or {@code storage} is not valid
   */
  static Replaceable newReplaceableMultimap(String storage, Path path, boolean madvise) {
    return newReplaceableMultimap(Storage.from(storage), new Storage.Params(path, madvise));
  }

  /**
   * Returns the values for a key. Safe to be called by multiple threads after all values have been written. After the
   * first read, all writes will fail.
   */
  LongArrayList get(long key);

  @Override
  void close();

  /**
   * A map from long to list of longs where you can use {@link #replaceValues(long, LongArrayList)} to set replace the
   * previous list of values with a new one.
   */
  interface Replaceable extends LongLongMultimap {

    /** Replaces the previous list of values for {@code key} with {@code values}. */
    void replaceValues(long key, LongArrayList values);
  }

  /**
   * A map from long to list of longs where you can use {@link #put(long, long)} or {@link #putAll(long, LongArrayList)}
   * to append values for a key.
   */
  interface Appendable extends LongLongMultimap {

    /**
     * Writes the value for a key. Not thread safe!
     */
    void put(long key, long value);

    default void putAll(long key, LongArrayList vals) {
      for (int i = 0; i < vals.size(); i++) {
        put(key, vals.get(i));
      }
    }
  }

  /** Dummy implementation of a map that throws an exception from {@link #get(long)}. */
  class Noop implements Replaceable, Appendable {

    @Override
    public void put(long key, long value) {}

    @Override
    public LongArrayList get(long key) {
      throw new UnsupportedOperationException("get(key) not implemented");
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return 0;
    }

    @Override
    public void close() {}

    @Override
    public void replaceValues(long key, LongArrayList values) {}
  }

  /**
   * A map from {@code long} to {@code long} stored as a list of keys and values that uses binary search to find the
   * values for a key. Inserts do not need to be ordered, the first read will sort the array.
   */
  class SparseUnorderedBinarySearchMultimap implements Appendable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparseUnorderedBinarySearchMultimap.class);

    private static final LongArrayList EMPTY_LIST = new LongArrayList();
    private final LongArrayList keys = new LongArrayList();
    private final LongArrayList values = new LongArrayList();
    private volatile boolean prepared = false;

    @Override
    public void put(long key, long val) {
      if (val <= 0) {
        throw new IllegalArgumentException("Invalid value: " + val + " must be >0");
      }
      if (prepared) {
        throw new IllegalArgumentException("Cannot insert after preparing");
      }
      keys.add(key);
      values.add(val);
    }

    private void prepare() {
      if (!prepared) {
        synchronized (this) {
          if (!prepared) {
            doPrepare();
            prepared = true;
          }
        }
      }
    }

    /** Sort the keys and values arrays by key */
    private void doPrepare() {
      Timer timer = Timer.start();

      LOGGER.debug("Sorting long long multimap...");
      long[] sortedKeys = keys.toArray();

      // this happens in a worker thread, but it's OK to use parallel sort because
      // all other threads will block while we prepare the multimap.
      Arrays.parallelSort(sortedKeys);

      // after sorting keys, sort values by iterating through each unordered key/value pair and
      // using binary search to find where to insert the result in sorted values.
      long[] sortedValues = new long[sortedKeys.length];
      int from = 0;
      while (from < keys.size()) {
        long key = keys.get(from);
        int to = Arrays.binarySearch(sortedKeys, key);
        if (to < 0) {
          throw new IllegalStateException("Key not found: " + key);
        }
        // skip back to the first entry for this key
        while (to >= 0 && sortedKeys[to] == key) {
          to--;
        }
        // skip ahead past values we've already added for this key
        do {
          to++;
        } while (sortedValues[to] != 0);
        while (from < keys.size() && keys.get(from) == key) {
          sortedValues[to++] = values.get(from++);
        }
      }
      keys.buffer = sortedKeys;
      values.buffer = sortedValues;
      LOGGER.debug("Sorted long long multimap " + timer.stop());
    }

    @Override
    public LongArrayList get(long key) {
      prepare();
      if (keys.isEmpty()) {
        return EMPTY_LIST;
      }
      int size = keys.size();
      int index = Arrays.binarySearch(keys.buffer, 0, size, key);
      LongArrayList result = new LongArrayList();
      if (index >= 0) {
        // binary search might drop us in the middle of repeated values, so look forwards...
        for (int i = index; i < size && keys.get(i) == key; i++) {
          result.add(values.get(i));
        }
        // ... and backwards to get all the matches
        for (int i = index - 1; i >= 0 && keys.get(i) == key; i--) {
          result.add(values.get(i));
        }
      }
      return result;
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return estimateSize(keys) + estimateSize(values);
    }

    @Override
    public void close() {
      keys.release();
      values.release();
    }
  }

  @Override
  default long diskUsageBytes() {
    return 0L;
  }

  /**
   * A map from {@code long} to {@code long} where each putAll replaces previous values and results are returned in the
   * same order they were inserted.
   */
  class DenseOrderedMultimap implements Replaceable {

    private static final LongArrayList EMPTY_LIST = new LongArrayList();
    private final LongIntHashMap keyToValuesIndex = Hppc.newLongIntHashMap();
    // each block starts with a "length" header then contains that number of entries
    private final AppendStore.Longs values;

    public DenseOrderedMultimap(Storage storage, Storage.Params params) {
      values = switch (storage) {
        case MMAP -> new AppendStoreMmap.Longs(params);
        case RAM -> new AppendStoreRam.Longs(false);
        case DIRECT -> new AppendStoreRam.Longs(true);
      };
    }

    @Override
    public void replaceValues(long key, LongArrayList values) {
      if (values.isEmpty()) {
        return;
      }
      keyToValuesIndex.put(key, (int) this.values.size());
      this.values.appendLong(values.size());
      for (int i = 0; i < values.size(); i++) {
        this.values.appendLong(values.get(i));
      }
    }

    @Override
    public LongArrayList get(long key) {
      int index = keyToValuesIndex.getOrDefault(key, -1);
      if (index >= 0) {
        LongArrayList result = new LongArrayList();
        int num = (int) values.getLong(index);
        for (int i = 0; i < num; i++) {
          result.add(values.getLong(i + index + 1));
        }
        return result;
      } else {
        return EMPTY_LIST;
      }
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return estimateSize(keyToValuesIndex) + estimateSize(values);
    }

    @Override
    public long diskUsageBytes() {
      return values.diskUsageBytes();
    }

    @Override
    public void close() {
      keyToValuesIndex.release();
      try {
        values.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
