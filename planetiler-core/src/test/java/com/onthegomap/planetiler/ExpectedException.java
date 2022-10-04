package com.onthegomap.planetiler;

/** An exception intentionally thrown by a test. */
public class ExpectedException extends RuntimeException {
  public ExpectedException() {
    super("expected exception", null, true, false);
  }
}
