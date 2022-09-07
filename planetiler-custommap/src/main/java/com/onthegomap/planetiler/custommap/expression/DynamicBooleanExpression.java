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
 */
public final class DynamicBooleanExpression implements Expression {
  private final ConfigExpression<ScriptContext, Boolean> expression;
  private final String expressionText;

  private DynamicBooleanExpression(String expression, ScriptContextDescription<ScriptContext> context) {
    expressionText = expression;
    this.expression = ConfigExpression.parse(expression, context, Boolean.class);
  }

  /** Creates a new boolean expression wrapping {@code expression}. */
  public static DynamicBooleanExpression dynamic(String expression, ScriptContextDescription<ScriptContext> context) {
    return new DynamicBooleanExpression(expression, context);
  }

  @Override
  public boolean evaluate(WithTags input, List<String> matchKeys) {
    return input instanceof ScriptContext context && expression.evaluate(context);
  }

  @Override
  public String generateJavaCode() {
    return "dynamic(" + Format.quote("${ " + expressionText + " }") + ")";
  }

  @Override
  public boolean equals(Object o) {
    return o == this || (o instanceof DynamicBooleanExpression e && Objects.equals(e.expressionText, expressionText));
  }

  @Override
  public int hashCode() {
    return expressionText.hashCode();
  }
}
