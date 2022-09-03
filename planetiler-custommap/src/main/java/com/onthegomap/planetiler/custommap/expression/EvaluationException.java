package com.onthegomap.planetiler.custommap.expression;

/**
 * Exception that occurs at runtime when evaluating an embedded expression.
 */
public class EvaluationException extends RuntimeException {

  public EvaluationException(String message, Exception cause) {
    super(message, cause);
  }

  public EvaluationException(String message) {
    super(message);
  }
}
