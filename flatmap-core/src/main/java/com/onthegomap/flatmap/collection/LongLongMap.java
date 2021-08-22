package com.onthegomap.flatmap.collection;

import com.carrotsearch.hppc.ByteArrayList;
import com.onthegomap.flatmap.util.DiskBacked;
import com.onthegomap.flatmap.util.FileUtils;
import com.onthegomap.flatmap.util.MemoryEstimator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public interface LongLongMap extends Closeable, MemoryEstimator.HasEstimate, DiskBacked {

  long MISSING_VALUE = Long.MIN_VALUE;

  static LongLongMap from(String name, String storage, Path path) {
    boolean ram = switch (storage) {
      case "ram" -> true;
      case "mmap" -> false;
      default -> throw new IllegalStateException("Unexpected storage value: " + storage);
    };

    return switch (name) {
      case "noop" -> noop();
      case "sortedtable" -> ram ? newInMemorySortedTable() : newDiskBackedSortedTable(path);
      case "sparsearray" -> ram ? newInMemorySparseArray() : newDiskBackedSparseArray(path);
      default -> throw new IllegalStateException("Unexpected value: " + name);
    };
  }

  static LongLongMap noop() {
    return new LongLongMap() {
      @Override
      public void put(long key, long value) {
      }

      @Override
      public long get(long key) {
        throw new UnsupportedOperationException("get");
      }

      @Override
      public long bytesOnDisk() {
        return 0;
      }

      @Override
      public void close() {
      }
    };
  }

  static LongLongMap newInMemorySortedTable() {
    return new SortedTable(
      new AppendStore.SmallLongs(i -> new AppendStoreRam.Ints()),
      new AppendStoreRam.Longs()
    );
  }

  static LongLongMap newDiskBackedSortedTable(Path dir) {
    FileUtils.createDirectory(dir);
    return new SortedTable(
      new AppendStore.SmallLongs(i -> new AppendStoreMmap.Ints(dir.resolve("keys-" + i))),
      new AppendStoreMmap.Longs(dir.resolve("values"))
    );
  }

  static LongLongMap newInMemorySparseArray() {
    return new SparseArray(new AppendStoreRam.Longs());
  }

  static LongLongMap newDiskBackedSparseArray(Path path) {
    return new SparseArray(new AppendStoreMmap.Longs(path));
  }

  void put(long key, long value);

  long get(long key);

  @Override
  default long bytesOnDisk() {
    return 0;
  }

  @Override
  default long estimateMemoryUsageBytes() {
    return 0;
  }

  default long[] multiGet(long[] key) {
    long[] result = new long[key.length];
    for (int i = 0; i < key.length; i++) {
      result[i] = get(key[i]);
    }
    return result;
  }

  class SortedTable implements LongLongMap {

    private final AppendStore.Longs offsets = new AppendStoreRam.Longs();
    private final AppendStore.Longs keys;
    private final AppendStore.Longs values;
    private long lastChunk = -1;

    public SortedTable(AppendStore.Longs keys, AppendStore.Longs values) {
      this.keys = keys;
      this.values = values;
    }

    @Override
    public void put(long key, long value) {
      long idx = keys.size();
      long chunk = key >>> 8;
      if (chunk != lastChunk) {
        while (offsets.size() <= chunk) {
          offsets.writeLong(idx);
        }
        lastChunk = chunk;
      }
      keys.writeLong(key);
      values.writeLong(value);
    }

    @Override
    public long get(long key) {
      long chunk = key >>> 8;
      if (chunk >= offsets.size()) {
        return MISSING_VALUE;
      }

      long lo = offsets.getLong(chunk);
      long hi = Math.min(keys.size(), chunk >= offsets.size() - 1 ? keys.size() : offsets.getLong(chunk + 1)) - 1;

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
    public long bytesOnDisk() {
      return keys.bytesOnDisk() + values.bytesOnDisk();
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return keys.estimateMemoryUsageBytes() + values.estimateMemoryUsageBytes() + MemoryEstimator.size(offsets);
    }

    @Override
    public void close() throws IOException {
      keys.close();
      values.close();
      offsets.close();
    }
  }

  class SparseArray implements LongLongMap {

    private final AppendStore.Longs offsets = new AppendStoreRam.Longs();
    private final ByteArrayList offsetStartPad = new ByteArrayList();
    private final AppendStore.Longs values;
    private int lastChunk = -1;
    private int lastOffset = 0;

    public SparseArray(AppendStore.Longs values) {
      this.values = values;
    }

    @Override
    public void put(long key, long value) {
      long idx = values.size();
      int chunk = (int) (key >>> 8);
      int offset = (int) (key & 255);

      if (chunk != lastChunk) {
        lastOffset = offset;
        while (offsets.size() <= chunk) {
          offsets.writeLong(idx);
          offsetStartPad.add((byte) offset);
        }
        lastChunk = chunk;
      } else {
        // in same chunk, write not_founds until we get to right idx
        while (++lastOffset < offset) {
          values.writeLong(MISSING_VALUE);
        }
      }
      values.writeLong(value);
    }

    @Override
    public long get(long key) {
      int chunk = (int) (key >>> 8);
      int offset = (int) (key & 255);
      if (chunk >= offsets.size()) {
        return MISSING_VALUE;
      }

      long lo = offsets.getLong(chunk);
      long hi = Math.min(values.size(), chunk >= offsets.size() - 1 ? values.size() : offsets.getLong(chunk + 1)) - 1;
      int startPad = offsetStartPad.get(chunk) & 255;

      long index = lo + offset - startPad;

      if (index > hi || index < lo) {
        return MISSING_VALUE;
      }

      return values.getLong(index);
    }

    @Override
    public long bytesOnDisk() {
      return values.bytesOnDisk();
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return values.estimateMemoryUsageBytes() + MemoryEstimator.size(offsets) + MemoryEstimator.size(offsetStartPad);
    }

    @Override
    public void close() throws IOException {
      offsetStartPad.release();
      values.close();
      offsets.close();
    }
  }
}
