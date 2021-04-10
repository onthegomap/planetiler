package com.onthegomap.flatmap.worker;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkQueue<T> implements Closeable, Supplier<T>, Consumer<T> {


  @Override
  public void close() throws IOException {

  }

  @Override
  public void accept(T t) {

  }

  @Override
  public T get() {
    return null;
  }
}

