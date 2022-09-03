package com.onthegomap.planetiler.custommap.expression;

/**
 * Exception that occurs at compile-time when preparing an embedded expression.
 */
public class ParseException extends RuntimeException {

  public ParseException(String message, Exception cause) {
    super(message, cause);
  }

  public ParseException(String message) {
    super(message);
  }
}
