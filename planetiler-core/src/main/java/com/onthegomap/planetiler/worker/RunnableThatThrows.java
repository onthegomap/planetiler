package com.onthegomap.planetiler.worker;

/**
 * A function that can throw checked exceptions.
 */
@FunctionalInterface
public interface RunnableThatThrows {

  void run() throws Exception;

  default void runAndWrapException() {
    try {
      run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
