package com.onthegomap.planetiler.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Caches the value of a function, so it only gets called once for each unique input, including when it throws an
 * exception.
 */
public class Memoized<I, O> implements Function<I, O> {
  private final ConcurrentHashMap<I, Try<O>> cache = new ConcurrentHashMap<>();
  private final Function<I, Try<O>> supplier;

  private Memoized(FunctionThatThrows<I, O> supplier) {
    this.supplier = i -> Try.apply(() -> supplier.apply(i));
  }

  /** Returns a memoized version of {@code supplier} that gets called only once for each input. */
  public static <I, O> Memoized<I, O> memoize(FunctionThatThrows<I, O> supplier) {
    return new Memoized<>(supplier);
  }


  @Override
  public O apply(I i) {
    Try<O> result = cache.get(i);
    if (result == null) {
      result = cache.computeIfAbsent(i, supplier);
    }
    return result.get();
  }

  /** Returns a success or failure wrapper for the function call. */
  public Try<O> tryApply(I i) {
    Try<O> result = cache.get(i);
    if (result == null) {
      result = cache.computeIfAbsent(i, supplier);
    }
    return result;
  }

  /** Returns a success or failure wrapper for the function call, and casting the result to {@code clazz}. */
  public <T> Try<T> tryApply(I i, Class<T> clazz) {
    return tryApply(i).cast(clazz);
  }
}
