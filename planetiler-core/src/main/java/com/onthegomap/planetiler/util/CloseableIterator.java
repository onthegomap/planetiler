package com.onthegomap.planetiler.util;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface CloseableIterator<T> extends Closeable, Iterator<T> {

  static <T> CloseableIterator<T> of(Stream<T> stream) {
    return new CloseableIterator<>() {
      private final Iterator<T> iter = stream.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public T next() {
        return iter.next();
      }

      @Override
      public void close() {
        stream.close();
      }
    };
  }

  @Override
  void close();

  default Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false).onClose(this::close);
  }

  default <O> CloseableIterator<O> map(Function<T, O> mapper) {
    var parent = this;
    return new CloseableIterator<>() {
      @Override
      public void close() {
        parent.close();
      }

      @Override
      public boolean hasNext() {
        return parent.hasNext();
      }

      @Override
      public O next() {
        return mapper.apply(parent.next());
      }
    };
  }

  default CloseableIterator<T> filter(Predicate<T> predicate) {
    final var parent = this;
    return new CloseableIterator<>() {
      private T nextValue;

      @Override
      public void close() {
        parent.close();
      }

      @Override
      public boolean hasNext() {
        if (nextValue != null) {
          return true;
        }
        while (parent.hasNext()) {
          final T parentNext = parent.next();
          if (predicate.test(parentNext)) {
            nextValue = parentNext;
            break;
          }
        }
        return nextValue != null;
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final T returnValue = nextValue;
        nextValue = null;
        return returnValue;
      }
    };
  }

}
