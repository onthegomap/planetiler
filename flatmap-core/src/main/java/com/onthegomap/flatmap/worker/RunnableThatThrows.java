package com.onthegomap.flatmap.worker;

/**
 * A function that can throw checked exceptions.
 */
@FunctionalInterface
public interface RunnableThatThrows {

  void run() throws Exception;
}
