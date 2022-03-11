package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.IntFunction;
import java.util.stream.Stream;

/**
 * A large array of primitives. A single thread appends all elements then allows random access from multiple threads.
 * <p>
 * {@link AppendStoreRam} stores all data in arrays in RAM and {@link AppendStoreMmap} stores all data in a
 * memory-mapped file.
 */
interface AppendStore extends Closeable, MemoryEstimator.HasEstimate, DiskBacked {

  /** Returns the number of elements in the array */
  long size();

  default void checkIndexInBounds(long index) {
    if (index >= size()) {
      throw new IndexOutOfBoundsException("index: " + index + " size: " + size());
    }
  }

  @Override
  default long estimateMemoryUsageBytes() {
    return 0;
  }

  @Override
  default long diskUsageBytes() {
    return 0;
  }

  /** An array of ints. */
  interface Ints extends AppendStore {

    static Ints create(Storage storage, Storage.Params params) {
      return switch (storage) {
        case DIRECT -> new AppendStoreDirect.Ints(params);
        case RAM -> new AppendStoreRam.Ints(params);
        case MMAP -> new AppendStoreMmap.Ints(params);
      };
    }

    void appendInt(int value);

    int getInt(long index);
  }

  /** An array of longs. */
  interface Longs extends AppendStore {

    static Longs create(Storage storage, Storage.Params params) {
      return switch (storage) {
        case DIRECT -> new AppendStoreDirect.Longs(params);
        case RAM -> new AppendStoreRam.Longs(params);
        case MMAP -> new AppendStoreMmap.Longs(params);
      };
    }

    void appendLong(long value);

    long getLong(long index);
  }

  /**
   * An array longs that uses 4 bytes to represent each long by using a list of {@link Ints} arrays. Only suitable for
   * values less than ~20 billion (i.e. OSM node IDs)
   */
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
    public void appendLong(long value) {
      int block = (int) (value >>> BITS);
      int offset = (int) (value & MASK);
      ints[block].appendInt(offset);
      numWritten++;
    }

    @Override
    public long getLong(long index) {
      checkIndexInBounds(index);
      for (int i = 0; i < ints.length; i++) {
        Ints slab = ints[i];
        long size = slab.size();
        if (index < slab.size()) {
          return slab.getInt(index) + (((long) i) << BITS);
        }
        index -= size;
      }
      throw new IndexOutOfBoundsException("index: " + index + " size: " + size());
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
    public long diskUsageBytes() {
      return Stream.of(ints).mapToLong(AppendStore::diskUsageBytes).sum();
    }
  }

}
