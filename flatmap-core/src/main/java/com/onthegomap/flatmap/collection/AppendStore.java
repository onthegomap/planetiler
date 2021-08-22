package com.onthegomap.flatmap.collection;

import com.onthegomap.flatmap.util.DiskBacked;
import com.onthegomap.flatmap.util.MemoryEstimator;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.IntFunction;
import java.util.stream.Stream;

interface AppendStore extends Closeable, MemoryEstimator.HasEstimate, DiskBacked {

  long size();

  default void checkIndex(long index) {
    if (index >= size()) {
      throw new IndexOutOfBoundsException("index: " + index + " size: " + size());
    }
  }

  @Override
  default long estimateMemoryUsageBytes() {
    return 0;
  }

  @Override
  default long bytesOnDisk() {
    return 0;
  }

  interface Ints extends AppendStore {

    void writeInt(int value);

    int getInt(long index);
  }

  interface Longs extends AppendStore {

    void writeLong(long value);

    long getLong(long index);
  }

  final class SmallLongs implements Longs {

    private static final int BITS = 31;
    private static final long MASK = (1L << BITS) - 1L;
    private final Ints[] ints = new Ints[10];
    private long numWritten = 0;

    SmallLongs(IntFunction<Ints> supplier) {
      for (int i = 0; i < ints.length; i++) {
        ints[i] = supplier.apply(i);
      }
    }

    @Override
    public void writeLong(long value) {
      int block = (int) (value >>> BITS);
      int offset = (int) (value & MASK);
      ints[block].writeInt(offset);
      numWritten++;
    }

    @Override
    public long getLong(long index) {
      for (int i = 0; i < ints.length; i++) {
        Ints slab = ints[i];
        long size = slab.size();
        if (index < slab.size()) {
          return slab.getInt(index) + (((long) i) << BITS);
        }
        index -= size;
      }
      throw new ArrayIndexOutOfBoundsException("index: " + index + " size: " + size());
    }

    @Override
    public long size() {
      return numWritten;
    }

    @Override
    public void close() throws IOException {
      for (var child : ints) {
        child.close();
      }
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return Stream.of(ints).mapToLong(AppendStore::estimateMemoryUsageBytes).sum();
    }

    @Override
    public long bytesOnDisk() {
      return Stream.of(ints).mapToLong(AppendStore::bytesOnDisk).sum();
    }
  }

}
