package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;

import java.io.IOException;

/**
 * A longlong map that only stores values and uses the key as an index into the array, with some tweaks to avoid storing
 * many sequential 0's.
 */
public class SparseArrayLongLongMap implements LongLongMap, LongLongMap.SequentialWrites {

  // The key space is broken into chunks of 256 and for each chunk with keys, store the index in the values array of
  // its first key shifted left 8 bits, plus the number of missing keys at the start of the chunk in the low 8 bits.
  // Only chunks with keys use memory, so small extracts don't pay for every chunk up to the largest node ID.
  private final SparseMonotonicLongMap offsets = new SparseMonotonicLongMap();

  private final AppendStore.Longs values;
  private int lastChunk = -1;
  private int lastOffset = 0;
  private long lastKey = -1;

  public SparseArrayLongLongMap(AppendStore.Longs values) {
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
      offsets.add(chunk, (idx << 8) | offset);
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
    long entry = offsets.get(chunk);
    if (entry < 0) {
      return MISSING_VALUE;
    }

    long lo = entry >>> 8;
    long next = offsets.next(chunk);
    long hi = (next < 0 ? values.size() : next >>> 8) - 1;
    int startPad = (int) (entry & 255);

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
    return values.estimateMemoryUsageBytes() + estimateSize(offsets);
  }

  @Override
  public void close() throws IOException {
    values.close();
  }
}
