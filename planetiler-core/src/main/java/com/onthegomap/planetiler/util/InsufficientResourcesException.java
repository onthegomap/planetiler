package com.onthegomap.planetiler.util;

/**
 * Thrown when a run stops because the machine doesn't have enough of a resource, like disk space or memory, with a
 * message that says what ran out and what to do about it.
 */
public class InsufficientResourcesException extends IllegalStateException {

  public InsufficientResourcesException(String message) {
    super(message);
  }

  public InsufficientResourcesException(String message, Throwable cause) {
    super(message, cause);
  }
}
