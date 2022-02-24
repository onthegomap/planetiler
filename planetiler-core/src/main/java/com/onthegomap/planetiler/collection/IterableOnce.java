package com.onthegomap.planetiler.collection;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * A {@link Supplier} that returns {@code null} when there are no elements left, with an {@link Iterable} view to
 * support for each loops.
 *
 * @param <T> Type of element returned
 */
public interface IterableOnce<T> extends Iterable<T>, Supplier<T> {

  @Override
  default Iterator<T> iterator() {
    return new Iterator<>() {
      T next = get();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public T next() {
        T result = next;
        next = get();
        return result;
      }
    };
  }
}
