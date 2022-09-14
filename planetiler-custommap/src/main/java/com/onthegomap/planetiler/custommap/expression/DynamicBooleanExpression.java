package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Format;
import java.util.List;
import java.util.Objects;
import org.projectnessie.cel.tools.Script;

/**
 * An {@link Expression} based off of a dynamic common-expression-language {@link Script common-expression-language
 * script}.
 *
 * @param <T> Input type of the expression
 */
public record DynamicBooleanExpression<T extends ScriptContext> (String expressionText,
  ConfigExpression<T, Boolean> expression, Class<T> inputClass) implements Expression {

  /** Creates a new boolean expression wrapping {@code expression}. */
  public static <T extends ScriptContext> DynamicBooleanExpression<T> dynamic(String expression,
    ScriptContextDescription<T> context) {
    var parsed = ConfigExpression.parse(expression, context, Boolean.class);
    return new DynamicBooleanExpression<>(expression, parsed, context.clazz());
  }

  @Override
  public boolean evaluate(WithTags input, List<String> matchKeys) {
    return inputClass.isInstance(input) && expression.apply(inputClass.cast(input));
  }

  @Override
  public String generateJavaCode() {
    return "dynamic(" + Format.quote("${ " + expressionText + " }") + ")";
  }

  @Override
  public boolean equals(Object o) {
    return o == this ||
      (o instanceof DynamicBooleanExpression<?> e && Objects.equals(e.expressionText, expressionText) &&
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
      return Boolean.TRUE.equals(result.item()) ? Expression.TRUE : Expression.FALSE;
    } else {
      return this;
    }
  }
}
