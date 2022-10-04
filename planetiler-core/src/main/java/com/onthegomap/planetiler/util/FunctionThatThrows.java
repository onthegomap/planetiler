package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

@FunctionalInterface
public interface FunctionThatThrows<I, O> {

  @SuppressWarnings("java:S112")
  O apply(I value) throws Exception;

  default O runAndWrapException(I value) {
    try {
      return apply(value);
    } catch (Exception e) {
      return throwFatalException(e);
    }
  }
}
