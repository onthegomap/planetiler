package com.onthegomap.planetiler.custommap.expression;

/**
 * Exception that occurs at compile-time when preparing an embedded expression.
 */
public class ParseException extends RuntimeException {

  public ParseException(String script, Exception cause) {
    super("Error parsing: %s".formatted(script), cause);
  }

  public ParseException(String message) {
    super(message);
  }
}
