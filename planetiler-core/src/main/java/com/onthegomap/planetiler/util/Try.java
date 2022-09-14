package com.onthegomap.planetiler.util;

public interface Try<T> {
  static <T> Try<T> apply(SupplierThatThrows<T> supplier) {
    try {
      return success(supplier.get());
    } catch (Exception e) {
      return failure(e);
    }
  }

  static <T> Success<T> success(T item) {
    return new Success<>(item);
  }

  static <T> Failure<T> failure(Exception throwable) {
    return new Failure<>(throwable);
  }

  T item();

  boolean isSuccess();

  boolean isFailure();

  record Success<T> (T item) implements Try<T> {

    @Override
    public boolean isSuccess() {
      return true;
    }

    @Override
    public boolean isFailure() {
      return false;
    }
  }
  record Failure<T> (Exception failure) implements Try<T> {

    @Override
    public T item() {
      throw new IllegalStateException(failure);
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public boolean isFailure() {
      return true;
    }
  }

  @FunctionalInterface
  interface SupplierThatThrows<T> {
    T get() throws Exception;
  }
}
