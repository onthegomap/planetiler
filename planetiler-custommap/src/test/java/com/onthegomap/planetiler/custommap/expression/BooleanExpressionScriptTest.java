package com.onthegomap.planetiler.custommap.expression;

import static com.onthegomap.planetiler.expression.Expression.and;
import static com.onthegomap.planetiler.expression.Expression.or;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.custommap.Contexts;
import com.onthegomap.planetiler.expression.Expression;
import org.junit.jupiter.api.Test;

class BooleanExpressionScriptTest {
  @Test
  void testSimplify() {
    assertEquals(Expression.TRUE,
      and(or(BooleanExpressionScript.script("1+1<3", Contexts.Root.DESCRIPTION))).simplify());
    assertEquals(Expression.FALSE,
      and(or(BooleanExpressionScript.script("1+1>3", Contexts.Root.DESCRIPTION))).simplify());

    var other = BooleanExpressionScript.script("feature.tags.natural", Contexts.ProcessFeature.DESCRIPTION);
    assertEquals(other, and(or(other)).simplify());
  }
}
