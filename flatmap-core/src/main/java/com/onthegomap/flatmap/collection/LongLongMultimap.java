package com.onthegomap.flatmap.collection;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntHashMap;
import com.graphhopper.util.StopWatch;
import com.onthegomap.flatmap.util.MemoryEstimator;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LongLongMultimap extends MemoryEstimator.HasEstimate {

  void put(long key, long value);

  LongArrayList get(long key);

  default void putAll(long key, LongArrayList vals) {
    for (int i = 0; i < vals.size(); i++) {
      put(key, vals.get(i));
    }
  }

  static LongLongMultimap newDensedOrderedMultimap() {
    return new DenseOrderedHppcMultimap();
  }

  static LongLongMultimap newSparseUnorderedMultimap() {
    return new SparseUnorderedBinarySearchMultimap();
  }

  class SparseUnorderedBinarySearchMultimap implements LongLongMultimap {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparseUnorderedBinarySearchMultimap.class);

    private static final LongArrayList EMPTY_LIST = new LongArrayList();
    private final LongArrayList keys = new LongArrayList();
    private final LongArrayList values = new LongArrayList();
    private volatile boolean prepared = false;
    private static final ThreadLocal<LongArrayList> resultHolder = ThreadLocal.withInitial(LongArrayList::new);

    protected LongArrayList getResultHolder() {
      LongArrayList res = resultHolder.get();
      res.elementsCount = 0;
      return res;
    }

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

    private void doPrepare() {
      StopWatch watch = new StopWatch().start();

      LOGGER.debug("Sorting long long multimap...");
      long[] sortedKeys = keys.toArray();

      // this happens in a worker thread, but it's OK to use parallel sort because
      // all other threads will block while we prepare the multimap.
      Arrays.parallelSort(sortedKeys);

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
      LongArrayList result = getResultHolder();
      if (index >= 0) {
        for (int i = index; i < size && keys.get(i) == key; i++) {
          result.add(values.get(i));
        }
        for (int i = index - 1; i >= 0 && keys.get(i) == key; i--) {
          result.add(values.get(i));
        }
      }
      return result;
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return MemoryEstimator.size(keys) + MemoryEstimator.size(values);
    }
  }

  class DenseOrderedHppcMultimap implements LongLongMultimap {

    private static final LongArrayList EMPTY_LIST = new LongArrayList();
    private final LongIntHashMap keys = new LongIntHashMap();
    private final LongArrayList values = new LongArrayList();

    @Override
    public void putAll(long key, LongArrayList others) {
      if (others.isEmpty()) {
        return;
      }
      keys.put(key, values.size());
      values.add(others.size());
      values.add(others.buffer, 0, others.size());
    }

    @Override
    public void put(long key, long val) {
      putAll(key, LongArrayList.from(val));
    }

    @Override
    public LongArrayList get(long key) {
      int index = keys.getOrDefault(key, -1);
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
      return MemoryEstimator.size(keys) + MemoryEstimator.size(values);
    }
  }
}
