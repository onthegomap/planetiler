package com.onthegomap.flatmap.collections;

import com.carrotsearch.hppc.LongArrayList;
import com.graphhopper.coll.GHLongLongHashMap;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.Format;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongSupplier;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.volume.ByteArrayVol;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LongLongMap extends Closeable {

  long MISSING_VALUE = Long.MIN_VALUE;

  void put(long key, long value);

  long get(long key);

  long fileSize();

  default long[] multiGet(long[] key) {
    long[] result = new long[key.length];
    for (int i = 0; i < key.length; i++) {
      result[i] = get(key[i]);
    }
    return result;
  }

  private static Volume prepare(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to delete " + path, e);
    }
    path.toFile().deleteOnExit();
    return MappedFileVol.FACTORY.makeVolume(path.toAbsolutePath().toString(), false);
  }

  private static Volume createInMemoryVolume() {
    return ByteArrayVol.FACTORY.makeVolume("", false);
  }

  static LongLongMap newFileBackedSortedTable(Path path) {
    Volume volume = prepare(path);
    return new MapdbSortedTable(volume, () -> FileUtils.size(path));
  }

  static LongLongMap newInMemorySortedTable() {
    Volume volume = createInMemoryVolume();
    return new MapdbSortedTable(volume, () -> 0);
  }

  static LongLongMap newFileBackedSparseArray(Path path) {
    return new SparseArray(path);
  }

  static LongLongMap newInMemorySparseArray(int segmentSize, int gapLimit) {
    return new SparseArrayMemory(segmentSize, gapLimit);
  }

  static LongLongMap newFileBackedSparseArray(Path path, int segmentSize, int gapLimit) {
    return new SparseArray(path, segmentSize, gapLimit);
  }

  static LongLongMap newArrayBacked() {
    return new Array();
  }

  static LongLongMap newInMemoryHashMap() {
    return new HppcMap();
  }

  class HppcMap implements LongLongMap {

    private final com.carrotsearch.hppc.LongLongMap underlying = new GHLongLongHashMap();

    @Override
    public void put(long key, long value) {
      underlying.put(key, value);
    }

    @Override
    public long get(long key) {
      return underlying.getOrDefault(key, MISSING_VALUE);
    }

    @Override
    public long fileSize() {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }
  }

  class MapdbSortedTable implements LongLongMap {

    private final SortedTableMap.Sink<Long, Long> mapSink;
    private volatile SortedTableMap<Long, Long> map = null;
    private final LongSupplier fileSize;

    private MapdbSortedTable(Volume volume, LongSupplier fileSize) {
      mapSink = SortedTableMap.create(volume, Serializer.LONG, Serializer.LONG).createFromSink();
      this.fileSize = fileSize;
    }

    private SortedTableMap<Long, Long> getMap() {
      SortedTableMap<Long, Long> result = map;
      if (result == null) {
        synchronized (this) {
          result = map;
          if (result == null) {
            map = mapSink.create();
          }
        }
      }
      return map;
    }

    @Override
    public void put(long key, long value) {
      mapSink.put(key, value);
    }

    @Override
    public long fileSize() {
      return fileSize.getAsLong();
    }

    @Override
    public long get(long key) {
      return getMap().getOrDefault(key, MISSING_VALUE);
    }

    @Override
    public void close() {
      if (map != null) {
        map.close();
      }
    }
  }

  class Array implements LongLongMap {

    int used = 0;
    private static final long MAX_MEM_USAGE = 100_000_000_000L; // 100GB
    private static final long SEGMENT_SIZE = 1_000_000; // 1MB
    private static final long SEGMENT_MAX_ENTRIES = SEGMENT_SIZE / 8 + 1;
    private static final long MAX_SEGMENTS = MAX_MEM_USAGE / SEGMENT_SIZE;

    private long[][] longs = new long[(int) MAX_SEGMENTS][];

    @Override
    public void put(long key, long value) {
      int segment = (int) (key / SEGMENT_MAX_ENTRIES);
      long[] seg = longs[segment];
      if (seg == null) {
        seg = longs[segment] = new long[(int) SEGMENT_MAX_ENTRIES];
        Arrays.fill(seg, MISSING_VALUE);
        used++;
      }
      seg[(int) (key % SEGMENT_MAX_ENTRIES)] = value;
    }

    @Override
    public long get(long key) {
      long[] segment = longs[(int) (key / SEGMENT_MAX_ENTRIES)];
      return segment == null ? MISSING_VALUE : segment[(int) (key % SEGMENT_MAX_ENTRIES)];
    }

    @Override
    public long fileSize() {
      return 24L + 8L * longs.length + ((long) used) * (24L + 8L * SEGMENT_MAX_ENTRIES);
    }

    @Override
    public void close() throws IOException {
      longs = null;
    }
  }

  class SparseArray implements LongLongMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparseArray.class);

    private static final int DEFAULT_CHUNK_SIZE = 1 << 8; // 256 (8 billion / (256mb / 8 bytes))
    private static final int DEFAULT_SEGMENT_SIZE_BYTES = 1 << 20; // 1MB
    private final long chunkSize;
    private final long segmentSize;
    private final Path path;
    private final DataOutputStream outputStream;
    private long lastKey;
    private long outIdx = 0;
    private FileChannel channel = null;
    private final LongArrayList keys = new LongArrayList();
    private volatile List<MappedByteBuffer> segments;

    SparseArray(Path path) {
      this(path, DEFAULT_SEGMENT_SIZE_BYTES, DEFAULT_CHUNK_SIZE);
    }

    public SparseArray(Path path, int segmentSize, int chunkSize) {
      this.path = path;
      this.segmentSize = segmentSize / 8;
      this.chunkSize = chunkSize;
      lastKey = -1L;
      try {
        this.outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 50_000));
        appendValue(MISSING_VALUE);
      } catch (IOException e) {
        throw new IllegalStateException("Could not create compact array output stream", e);
      }
    }

    @Override
    public void put(long key, long value) {
      assert key > lastKey;
      int chunk = (int) (key / chunkSize);

      try {
        if (chunk >= keys.elementsCount) {
          while (chunk >= keys.elementsCount) {
            keys.add(outIdx);
          }
          lastKey = chunk * chunkSize;
        } else {
          lastKey++;
        }
        for (; lastKey < key; lastKey++) {
          appendValue(MISSING_VALUE);
        }
        appendValue(value);
      } catch (IOException e) {
        throw new IllegalStateException("Could not put value", e);
      }
    }

    private void appendValue(long value) throws IOException {
      outIdx++;
      outputStream.writeLong(value);
    }

    @Override
    public long get(long key) {
      if (segments == null) {
        synchronized (this) {
          if (segments == null) {
            build();
          }
        }
      }
      int chunk = (int) (key / chunkSize);
      if (key > lastKey || chunk >= keys.elementsCount) {
        return MISSING_VALUE;
      }
      long start = keys.get(chunk);
      long fileIdx = start + key % chunkSize;
      if (chunk < keys.elementsCount) {
        long next = keys.get(chunk + 1);
        if (fileIdx >= next) {
          return MISSING_VALUE;
        }
      } else {
        return MISSING_VALUE;
      }
      return getValue(fileIdx);
    }

    private void build() {
      try {
        keys.add(outIdx);
        outputStream.close();
        channel = FileChannel.open(path, StandardOpenOption.READ);
        var segmentCount = (int) (outIdx / segmentSize + 1);
        List<MappedByteBuffer> result = new ArrayList<>(segmentCount);
        LOGGER.info("LongLongMap.SparseArray gaps=" + Format.formatInteger(keys.size()) +
          " segments=" + Format.formatInteger(segmentCount));
        for (long offset = 0; offset < outIdx; offset += segmentSize) {
          result
            .add(
              channel
                .map(FileChannel.MapMode.READ_ONLY, offset << 3,
                  Math.min(segmentSize, outIdx - offset) << 3));
        }
        segments = result;
      } catch (IOException e) {
        throw new IllegalStateException("Could not create segments", e);
      }
    }

    private long getValue(long fileIdx) {
      int segNum = (int) (fileIdx / segmentSize);
      int segOffset = (int) (fileIdx % segmentSize);
      return segments.get(segNum).getLong(segOffset << 3);
    }

    private int binarySearch(long key) {
      return Arrays.binarySearch(keys.buffer, 0, keys.elementsCount, key);
    }

    @Override
    public long fileSize() {
      return FileUtils.size(path);
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
      channel.close();
    }
  }

  class SparseArrayMemory implements LongLongMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparseArrayMemory.class);

    private static final int DEFAULT_CHUNK_SIZE = 1 << 8; // 256 (8 billion / (256mb / 8 bytes))
    private static final int DEFAULT_SEGMENT_SIZE_BYTES = 1 << 20; // 1MB
    private final long chunkSize;
    private final long segmentSize;
    private long lastKey;
    private long outIdx = 0;
    private final LongArrayList keys = new LongArrayList();
    private final List<LongArrayList> segments = new ArrayList<>();

    SparseArrayMemory() {
      this(DEFAULT_SEGMENT_SIZE_BYTES, DEFAULT_CHUNK_SIZE);
    }

    public SparseArrayMemory(int segmentSize, int chunkSize) {
      this.segmentSize = segmentSize / 8;
      this.chunkSize = chunkSize;
      lastKey = -1L;
      segments.add(new LongArrayList());
      appendValue(MISSING_VALUE);
    }

    @Override
    public void put(long key, long value) {
      assert key > lastKey;
      int chunk = (int) (key / chunkSize);

      if (chunk >= keys.elementsCount) {
        while (chunk >= keys.elementsCount) {
          keys.add(outIdx);
        }
        lastKey = chunk * chunkSize;
      } else {
        lastKey++;
      }
      for (; lastKey < key; lastKey++) {
        appendValue(MISSING_VALUE);
      }
      appendValue(value);
    }

    private void appendValue(long value) {
      outIdx++;
      var last = segments.get(segments.size() - 1);
      if (last.size() >= segmentSize) {
        segments.add(last = new LongArrayList());
      }
      last.add(value);
    }

    private volatile boolean init = false;

    @Override
    public long get(long key) {
      if (!init) {
        synchronized (this) {
          if (!init) {
            keys.add(outIdx);
            init = true;
          }
        }
      }
      int chunk = (int) (key / chunkSize);
      if (key > lastKey || chunk >= keys.elementsCount) {
        return MISSING_VALUE;
      }
      long start = keys.get(chunk);
      long fileIdx = start + key % chunkSize;
      if (chunk < keys.elementsCount) {
        long next = keys.get(chunk + 1);
        if (fileIdx >= next) {
          return MISSING_VALUE;
        }
      } else {
        return MISSING_VALUE;
      }
      return getValue(fileIdx);
    }

    private long getValue(long fileIdx) {
      int segNum = (int) (fileIdx / segmentSize);
      int segOffset = (int) (fileIdx % segmentSize);
      return segments.get(segNum).get(segOffset);
    }

    @Override
    public long fileSize() {
      return 0L;
    }

    @Override
    public void close() throws IOException {
      keys.release();
      segments.forEach(LongArrayList::release);
      segments.clear();
    }
  }
}
