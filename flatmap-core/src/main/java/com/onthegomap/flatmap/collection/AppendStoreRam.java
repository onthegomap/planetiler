package com.onthegomap.flatmap.collection;

import static com.onthegomap.flatmap.util.MemoryEstimator.POINTER_BYTES;
import static com.onthegomap.flatmap.util.MemoryEstimator.estimateIntArraySize;
import static com.onthegomap.flatmap.util.MemoryEstimator.estimateLongArraySize;

import java.util.ArrayList;
import java.util.List;

/**
 * An array of primitives backed by arrays in RAM.
 *
 * @param <T> the primitive array type (i.e. {@code int[]} or {@code long[]})
 */
abstract class AppendStoreRam<T> implements AppendStore {

  final List<T> arrays;
  long size = 0;
  final int slabSize;
  final int slabBits;
  final long slabMask;

  AppendStoreRam(int segmentSizeBytes) {
    this.slabBits = (int) (Math.log(segmentSizeBytes) / Math.log(2));
    if (1 << slabBits != segmentSizeBytes) {
      throw new IllegalArgumentException("Segment size must be a power of 2: " + segmentSizeBytes);
    }
    this.slabSize = (1 << slabBits);
    this.slabMask = slabSize - 1;
    this.arrays = new ArrayList<>();
  }

  /** Returns the slab that the next value should be written to. */
  T getSlabForWrite() {
    int slabIdx = (int) (size >>> slabBits);
    while (arrays.size() <= slabIdx) {
      arrays.add(newSlab());
    }
    return arrays.get(slabIdx);
  }

  abstract T newSlab();

  @Override
  public long size() {
    return size;
  }

  @Override
  public void close() {
    arrays.clear();
  }

  static class Ints extends AppendStoreRam<int[]> implements AppendStore.Ints {

    Ints() {
      this(1 << 20); // 1MB
    }

    Ints(int segmentSizeBytes) {
      super(segmentSizeBytes >>> 2);
    }

    @Override
    int[] newSlab() {
      return new int[slabSize];
    }

    @Override
    public void appendInt(int value) {
      int offset = (int) (size & slabMask);
      getSlabForWrite()[offset] = value;
      size++;
    }

    @Override
    public int getInt(long index) {
      checkIndexInBounds(index);
      int slabIdx = (int) (index >>> slabBits);
      int offset = (int) (index & slabMask);
      int[] slab = arrays.get(slabIdx);
      return slab[offset];
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return arrays.size() * (estimateIntArraySize(slabSize) + POINTER_BYTES);
    }
  }

  static class Longs extends AppendStoreRam<long[]> implements AppendStore.Longs {

    Longs() {
      this(1 << 20); // 1MB
    }

    Longs(int segmentSizeBytes) {
      super(segmentSizeBytes >>> 3);
    }

    @Override
    protected long[] newSlab() {
      return new long[slabSize];
    }

    @Override
    public void appendLong(long value) {
      int offset = (int) (size & slabMask);
      getSlabForWrite()[offset] = value;
      size++;
    }

    @Override
    public long getLong(long index) {
      checkIndexInBounds(index);
      int slabIdx = (int) (index >>> slabBits);
      int offset = (int) (index & slabMask);
      return arrays.get(slabIdx)[offset];
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return arrays.size() * (estimateLongArraySize(slabSize) + POINTER_BYTES);
    }
  }
}
