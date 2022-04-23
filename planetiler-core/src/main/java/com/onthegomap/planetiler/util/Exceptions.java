package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Exception-handling utilities.
 */
public class Exceptions {
  private Exceptions() {}

  /**
   * Re-throw a caught exception, handling interrupts and wrapping in a {@link FatalPlanetilerException} if checked.
   *
   * @param exception The original exception
   * @param <T>       Return type if caller requires it
   */
  public static <T> T throwFatalException(Throwable exception) {
    if (exception instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    if (exception instanceof RuntimeException runtimeException) {
      throw runtimeException;
    } else if (exception instanceof IOException ioe) {
      throw new UncheckedIOException(ioe);
    } else if (exception instanceof Error error) {
      throw error;
    }
    throw new FatalPlanetilerException(exception);
  }

  /**
   * Fatal exception that will result in planetiler exiting early and shutting down.
   */
  public static class FatalPlanetilerException extends RuntimeException {
    public FatalPlanetilerException(Throwable exception) {
      super(exception);
    }
  }
}
