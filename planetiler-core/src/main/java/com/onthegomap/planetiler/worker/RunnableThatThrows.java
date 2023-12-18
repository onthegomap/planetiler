package com.onthegomap.planetiler.worker;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

/**
 * A function that can throw checked exceptions.
 */
@FunctionalInterface
public interface RunnableThatThrows {

  @SuppressWarnings("java:S112")
  void run() throws Exception;

  default void runAndWrapException() {
    try {
      run();
    } catch (Exception e) {
      throwFatalException(e);
    }
  }

  static Runnable wrap(RunnableThatThrows thrower) {
    return thrower::runAndWrapException;
  }
}
