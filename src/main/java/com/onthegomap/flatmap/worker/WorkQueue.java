package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.stats.Stats;
import java.io.Closeable;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkQueue<T> implements Closeable, Supplier<T>, Consumer<T> {

  public WorkQueue(String name, int capacity, int maxBatch, Stats stats) {

  }

  @Override
  public void close() {

  }

  @Override
  public void accept(T t) {

  }

  @Override
  public T get() {
    return null;
  }
}

