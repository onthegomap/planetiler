package com.onthegomap.flatmap.collections;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface LongLongMap extends Closeable {

  void put(long key, long value);

  long get(long key);

  File filePath();

  class MapdbSortedTable implements LongLongMap {

    public MapdbSortedTable(File nodeDb) {

    }

    @Override
    public void put(long key, long value) {

    }

    @Override
    public long get(long key) {
      return 0;
    }

    @Override
    public File filePath() {
      return null;
    }

    @Override
    public void close() throws IOException {

    }
  }
}
