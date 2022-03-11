package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MemoryEstimator.POINTER_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.estimateIntArraySize;
import static com.onthegomap.planetiler.util.MemoryEstimator.estimateLongArraySize;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * An array of primitives backed direct off-heap {@link ByteBuffer ByteBuffers}.
 */
abstract class AppendStoreDirect implements AppendStore {

  final List<ByteBuffer> arrays;
  long size = 0;
  final int slabSize;
  final int slabBits;
  final long slabMask;

  AppendStoreDirect(int segmentSizeBytes) {
    this.slabBits = (int) (Math.log(segmentSizeBytes) / Math.log(2));
    if (1 << slabBits != segmentSizeBytes) {
      throw new IllegalArgumentException("Segment size must be a power of 2: " + segmentSizeBytes);
    }
    if (segmentSizeBytes % 8 != 0) {
      throw new IllegalStateException("Segment size must be a multiple of 8: " + segmentSizeBytes);
    }
    this.slabSize = (1 << slabBits);
    this.slabMask = slabSize - 1;
    this.arrays = new ArrayList<>();
  }

  /** Returns the slab that the next value should be written to. */
  ByteBuffer getSlabForWrite() {
    int slabIdx = (int) (size >>> slabBits);
    while (arrays.size() <= slabIdx) {
      arrays.add(newSlab());
    }
    return arrays.get(slabIdx);
  }

  ByteBuffer newSlab() {
    return ByteBuffer.allocateDirect(slabSize);
  }

  @Override
  public void close() {
    arrays.clear();
  }

  static class Ints extends AppendStoreDirect implements AppendStore.Ints {

    Ints(Storage.Params params) {
      this();
    }

    Ints() {
      this(1 << 20); // 1MB
    }

    Ints(int segmentSizeBytes) {
      super(segmentSizeBytes);
    }

    @Override
    public void appendInt(int value) {
      int offset = (int) (size & slabMask);
      getSlabForWrite().putInt(offset, value);
      size += Integer.BYTES;
    }

    @Override
    public int getInt(long index) {
      checkIndexInBounds(index);
      long byteIndex = index << 2;
      int slabIdx = (int) (byteIndex >>> slabBits);
      int offset = (int) (byteIndex & slabMask);
      return arrays.get(slabIdx).getInt(offset);
    }

    @Override
    public long size() {
      return size >> 2;
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return arrays.size() * (estimateIntArraySize(slabSize) + POINTER_BYTES);
    }
  }

  static class Longs extends AppendStoreDirect implements AppendStore.Longs {

    Longs(Storage.Params params) {
      this();
    }

    Longs() {
      this(1 << 20); // 1MB
    }

    Longs(int segmentSizeBytes) {
      super(segmentSizeBytes);
    }

    @Override
    public void appendLong(long value) {
      int offset = (int) (size & slabMask);
      getSlabForWrite().putLong(offset, value);
      size += Long.BYTES;
    }

    @Override
    public long getLong(long index) {
      checkIndexInBounds(index);
      long byteIndex = index << 3;
      int slabIdx = (int) (byteIndex >>> slabBits);
      int offset = (int) (byteIndex & slabMask);
      return arrays.get(slabIdx).getLong(offset);
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return arrays.size() * (estimateLongArraySize(slabSize) + POINTER_BYTES);
    }

    @Override
    public long size() {
      return size >> 3;
    }
  }
}
