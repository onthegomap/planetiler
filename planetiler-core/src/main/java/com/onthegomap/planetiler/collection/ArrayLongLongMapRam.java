package com.onthegomap.planetiler.collection;

import java.io.IOException;

public class ArrayLongLongMapRam implements LongLongMap.ParallelWrites {

  @Override
  public Writer newWriter() {
    return null;
  }

  @Override
  public long get(long key) {
    return 0;
  }

  @Override
  public long diskUsageBytes() {
    return ParallelWrites.super.diskUsageBytes();
  }

  @Override
  public long estimateMemoryUsageBytes() {
    return ParallelWrites.super.estimateMemoryUsageBytes();
  }

  @Override
  public long[] multiGet(long[] key) {
    return ParallelWrites.super.multiGet(key);
  }

  @Override
  public void close() throws IOException {
    
  }
}
