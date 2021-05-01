package com.onthegomap.flatmap.collections;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public interface LongLongMap extends Closeable {

  void put(long key, long value);

  long get(long key);

  Path filePath();

  class MapdbSortedTable implements LongLongMap {

    public MapdbSortedTable(Path nodeDb) {

    }

    @Override
    public void put(long key, long value) {

    }

    @Override
    public long get(long key) {
      return 0;
    }

    @Override
    public Path filePath() {
      return null;
    }

    @Override
    public void close() throws IOException {

    }
  }
}
