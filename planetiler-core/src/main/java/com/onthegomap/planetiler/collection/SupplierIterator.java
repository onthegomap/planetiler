package com.onthegomap.planetiler.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Adapts a {@link Supplier} that returns {@code null} when no items are left to an {@link Iterator} where
 * {@link #hasNext()} returns {@code false} when there are no items left.
 */
public class SupplierIterator<T> implements Iterator<T> {
  private final Supplier<T> supplier;
  T next = null;
  boolean stale = true;

  public SupplierIterator(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  private void advance() {
    if (stale) {
      next = supplier.get();
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
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    stale = true;
    return next;
  }
}
