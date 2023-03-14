package com.onthegomap.planetiler.util;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface CloseableIterator<T> extends Closeable, Iterator<T> {

  @Override
  void close();

  default Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false).onClose(this::close);
  }
}
