package com.onthegomap.planetiler;

/** An exception intentionally thrown by a test. */
public class ExpectedException extends Error {
  public ExpectedException() {
    super("expected exception", null, true, false);
  }
}
