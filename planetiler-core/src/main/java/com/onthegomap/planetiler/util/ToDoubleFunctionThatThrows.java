package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

@FunctionalInterface
public interface ToDoubleFunctionThatThrows<I> {

  @SuppressWarnings("java:S112")
  double applyAsDouble(I value) throws Exception;

  default double applyAndWrapException(I value) {
    try {
      return applyAsDouble(value);
    } catch (Exception e) {
      return throwFatalException(e);
    }
  }
}
