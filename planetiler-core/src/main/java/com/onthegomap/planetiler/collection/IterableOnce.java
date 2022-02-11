package com.onthegomap.planetiler.collection;

import java.util.Iterator;
import java.util.function.Supplier;

public interface IterableOnce<T> extends Iterable<T>, Supplier<T> {

  static <T> Iterator<T> iterateThrough(Supplier<T> supplier) {
    return new Iterator<>() {
      T next = supplier.get();

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public T next() {
        T result = next;
        next = supplier.get();
        return result;
      }
    };
  }

  static <T> Iterable<T> of(Supplier<T> supplier) {
    return () -> iterateThrough(supplier);
  }

  @Override
  default Iterator<T> iterator() {
    return iterateThrough(this);
  }
}
