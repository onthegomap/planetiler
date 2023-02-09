package com.onthegomap.planetiler.worker;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

/**
 * A consumer that can throw checked exceptions.
 */
@FunctionalInterface
public interface ConsumerThatThrows<T> {

  @SuppressWarnings("java:S112")
  void accept(T value) throws Exception;

  default void runAndWrapException(T value) {
    try {
      accept(value);
    } catch (Exception e) {
      throwFatalException(e);
    }
  }
}
