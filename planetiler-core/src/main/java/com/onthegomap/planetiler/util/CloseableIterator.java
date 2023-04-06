package com.onthegomap.planetiler.util;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface CloseableIterator<T> extends Closeable, Iterator<T> {

  static <T> CloseableIterator<T> wrap(Iterator<T> iter) {
    return new CloseableIterator<>() {
      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public T next() {
        return iter.next();
      }
    };
  }

  @Override
  void close();

  default Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false).onClose(this::close);
  }

  default List<T> toList() {
    try (this) {
      List<T> result = new ArrayList<>();
      while (hasNext()) {
        result.add(next());
      }
      return result;
    }
  }
}
