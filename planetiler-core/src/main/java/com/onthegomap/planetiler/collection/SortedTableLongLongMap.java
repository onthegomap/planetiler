package com.onthegomap.planetiler.collection;

import java.io.IOException;

/**
 * A longlong map that stores keys and values sorted by key and does a binary search to lookup values.
 */
public class SortedTableLongLongMap implements LongLongMap, LongLongMap.SequentialWrites {

  /*
   * It's not actually a binary search, it keeps track of the first index of each block of 256 keys, so it
   * can do an O(1) lookup to narrow down the search space to 256 values.
   */
  // only chunks with keys use memory, so small extracts don't pay for every chunk up to the largest node ID
  private final SparseMonotonicLongMap offsets = new SparseMonotonicLongMap();
  private final AppendStore.Longs keys;
  private final AppendStore.Longs values;
  private long lastChunk = -1;
  private long lastKey = -1;

  public SortedTableLongLongMap(AppendStore.Longs keys, AppendStore.Longs values) {
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
      offsets.add(chunk, idx);
      lastChunk = chunk;
    }
    keys.appendLong(key);
    values.appendLong(value);
  }

  @Override
  public long get(long key) {
    long chunk = key >>> 8;
    // use the "offsets" index to narrow search space to <256 values
    long lo = offsets.get(chunk);
    if (lo < 0) {
      return MISSING_VALUE;
    }
    long next = offsets.next(chunk);
    long hi = (next < 0 ? keys.size() : next) - 1;

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
  }
}
