package com.onthegomap.flatmap.collections;

import com.graphhopper.coll.GHLongLongHashMap;
import com.onthegomap.flatmap.FileUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongSupplier;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.volume.ByteArrayVol;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;

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
}
