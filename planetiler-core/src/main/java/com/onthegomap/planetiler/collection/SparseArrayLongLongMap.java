package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;

import com.carrotsearch.hppc.ByteArrayList;
import java.io.IOException;

/**
 * A longlong map that only stores values and uses the key as an index into the array, with some tweaks to avoid storing
 * many sequential 0's.
 */
public class SparseArrayLongLongMap implements LongLongMap, LongLongMap.SequentialWrites {

  // The key space is broken into chunks of 256 and for each chunk, store:
  // 1) the index in the outputs array for the first key in the block
  private final AppendStore.Longs offsets = new AppendStoreRam.Longs(false);
  // 2) the number of leading 0's at the start of each block
  private final ByteArrayList offsetStartPad = new ByteArrayList();

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
    long hi = Math.min(values.size(), chunk >= offsets.size() - 1 ? values.size() : offsets.getLong(chunk + 1L)) - 1;
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
