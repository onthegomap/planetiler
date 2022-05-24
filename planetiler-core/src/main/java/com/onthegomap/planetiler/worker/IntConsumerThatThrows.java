package com.onthegomap.planetiler.worker;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

/**
 * A function that takes an integer can throw checked exceptions.
 */
@FunctionalInterface
public interface IntConsumerThatThrows {

  @SuppressWarnings("java:S112")
  void accept(int value) throws Exception;

  default void runAndWrapException(int value) {
    try {
      accept(value);
    } catch (Exception e) {
      throwFatalException(e);
    }
  }
}
