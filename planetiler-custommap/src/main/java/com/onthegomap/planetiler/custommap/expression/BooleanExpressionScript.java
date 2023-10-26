package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Format;
import java.util.List;
import java.util.Objects;

/**
 * A boolean {@link Expression} based off of a dynamic expression script parsed from a string.
 *
 * @param expression     The parsed CEL script
 * @param expressionText The original CEL script string to evaluate
 * @param inputClass     Type of the context that the script is expecting.
 *
 * @param <T>            Type of the expression context
 */
public record BooleanExpressionScript<T extends ScriptContext>(
  String expressionText,
  ConfigExpressionScript<T, Boolean> expression,
  Class<T> inputClass
) implements Expression {

  /** Creates a new boolean expression from {@code script} where {@code context} defines the available variables. */
  public static <T extends ScriptContext> BooleanExpressionScript<T> script(String script,
    ScriptEnvironment<T> context) {
    var parsed = ConfigExpressionScript.parse(script, context, Boolean.class);
    return new BooleanExpressionScript<>(script, parsed, context.clazz());
  }

  @Override
  public boolean evaluate(WithTags input, List<String> matchKeys) {
    return inputClass.isInstance(input) && expression.apply(inputClass.cast(input));
  }

  @Override
  public String generateJavaCode() {
    return "script(" + Format.quote("${ " + expressionText + " }") + ")";
  }

  @Override
  public boolean equals(Object o) {
    return o == this ||
      (o instanceof BooleanExpressionScript<?> e && Objects.equals(e.expressionText, expressionText) &&
        Objects.equals(e.inputClass, inputClass));
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressionText, inputClass);
  }

  @Override
  public Expression simplifyOnce() {
    var result = expression.tryStaticEvaluate();
    if (result.isSuccess()) {
      return Boolean.TRUE.equals(result.get()) ? Expression.TRUE : Expression.FALSE;
    } else {
      return this;
    }
  }
}
