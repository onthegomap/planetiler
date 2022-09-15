package com.onthegomap.planetiler.util;

/**
 * A container for the result of an operation that may succeed or fail.
 *
 * @param <T> Type of the result value, if success
 */
public interface Try<T> {
  /**
   * Calls {@code supplier} and wraps the result in {@link Success} if successful, or {@link Failure} if it throws an
   * exception.
   */
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

  /**
   * Returns the result if success, or throws an exception if failure.
   *
   * @throws IllegalStateException wrapping the exception on failure
   */
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
    @SuppressWarnings("java:S112")
    T get() throws Exception;
  }
}
