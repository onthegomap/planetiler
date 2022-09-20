package com.onthegomap.planetiler.custommap.expression;

/**
 * Exception that occurs at runtime when evaluating a {@link ConfigExpressionScript}.
 */
public class EvaluationException extends RuntimeException {

  public EvaluationException(String script, Exception cause) {
    super("Error evaluating script: %s".formatted(script), cause);
  }
}
