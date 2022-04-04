package com.onthegomap.planetiler.collection;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * A {@link Supplier} that returns {@code null} when there are no elements left, with an {@link Iterable} view to
 * support for each loop.
 *
 * @param <T> Type of element returned
 */
public interface IterableOnce<T> extends Iterable<T>, Supplier<T> {

  @Override
  default Iterator<T> iterator() {
    return new Iterator<>() {
      T next = null;
      boolean stale = true;

      private void advance() {
        if (stale) {
          next = get();
          stale = false;
        }
      }

      @Override
      public boolean hasNext() {
        advance();
        return next != null;
      }

      @Override
      public T next() {
        advance();
        stale = true;
        return next;
      }
    };
  }
}
