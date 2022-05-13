package com.onthegomap.planetiler.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class SupplierIterator<T> implements Iterator<T> {
  private final Supplier<T> supplier;

  public SupplierIterator(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  T next = null;
  boolean stale = true;

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
