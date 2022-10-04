package com.onthegomap.planetiler;

/** A fatal error intentionally thrown by a test. */
public class ExpectedError extends Error {
  public ExpectedError() {
    super("expected error", null, true, false);
  }
}
