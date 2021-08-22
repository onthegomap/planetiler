package com.onthegomap.flatmap.collection;

import java.util.ArrayList;
import java.util.List;

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

  T writeSlab() {
    long idx = size++;
    int slabIdx = (int) (idx >>> slabBits);
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
    public void writeInt(int value) {
      int offset = (int) (size & slabMask);
      writeSlab()[offset] = value;
    }

    @Override
    public int getInt(long index) {
      checkIndex(index);
      int slabIdx = (int) (index >>> slabBits);
      int offset = (int) (index & slabMask);
      int[] slab = arrays.get(slabIdx);
      return slab[offset];
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return arrays.size() * (slabSize * 4L + 24L + 8);
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
    public void writeLong(long value) {
      int offset = (int) (size & slabMask);
      writeSlab()[offset] = value;
    }

    @Override
    public long getLong(long index) {
      checkIndex(index);
      int slabIdx = (int) (index >>> slabBits);
      int offset = (int) (index & slabMask);
      return arrays.get(slabIdx)[offset];
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return arrays.size() * (slabSize * 8L + 24L + 8);
    }
  }
}
