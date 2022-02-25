package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntHashMap;
import com.graphhopper.util.StopWatch;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-memory map that stores a multiple {@code long} values for each {@code long} key.
 */
// TODO: The two implementations should probably not implement the same interface
public interface LongLongMultimap extends MemoryEstimator.HasEstimate {

  /**
   * Writes the value for a key. Not thread safe!
   */
  void put(long key, long value);

  /**
   * Returns the values for a key. Safe to be called by multiple threads after all values have been written. After the
   * first read, all writes will fail.
   */
  LongArrayList get(long key);

  default void putAll(long key, LongArrayList vals) {
    for (int i = 0; i < vals.size(); i++) {
      put(key, vals.get(i));
    }
  }

  /** Returns a new multimap where each write sets the list of values for a key, and that order is preserved on read. */
  static LongLongMultimap newDensedOrderedMultimap() {
    return new DenseOrderedHppcMultimap();
  }

  /** Returns a new multimap where each write adds a value for the given key. */
  static LongLongMultimap newSparseUnorderedMultimap() {
    return new SparseUnorderedBinarySearchMultimap();
  }

  /**
   * A map from {@code long} to {@code long} stored as a list of keys and values that uses binary search to find the
   * values for a key. Inserts do not need to be ordered, the first read will sort the array.
   */
  class SparseUnorderedBinarySearchMultimap implements LongLongMultimap {

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
      StopWatch watch = new StopWatch().start();

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
      LOGGER.debug("Sorted long long multimap " + watch.stop());
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
  }

  /**
   * A map from {@code long} to {@code long} where each putAll replaces previous values and results are returned in the
   * same order they were inserted.
   */
  class DenseOrderedHppcMultimap implements LongLongMultimap {

    private static final LongArrayList EMPTY_LIST = new LongArrayList();
    private final LongIntHashMap keyToValuesIndex = new LongIntHashMap();
    // each block starts with a "length" header then contains that number of entries
    private final LongArrayList values = new LongArrayList();

    @Override
    public void putAll(long key, LongArrayList others) {
      if (others.isEmpty()) {
        return;
      }
      keyToValuesIndex.put(key, values.size());
      values.add(others.size());
      values.add(others.buffer, 0, others.size());
    }

    @Override
    public void put(long key, long val) {
      putAll(key, LongArrayList.from(val));
    }

    @Override
    public LongArrayList get(long key) {
      int index = keyToValuesIndex.getOrDefault(key, -1);
      if (index >= 0) {
        LongArrayList result = new LongArrayList();
        int num = (int) values.get(index);
        result.add(values.buffer, index + 1, num);
        return result;
      } else {
        return EMPTY_LIST;
      }
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return estimateSize(keyToValuesIndex) + estimateSize(values);
    }
  }
}
