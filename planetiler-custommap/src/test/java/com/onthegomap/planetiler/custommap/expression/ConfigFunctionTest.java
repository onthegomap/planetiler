package com.onthegomap.planetiler.custommap.expression;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.custommap.expression.ConfigFunction.*;
import static com.onthegomap.planetiler.expression.Expression.matchAny;
import static com.onthegomap.planetiler.expression.Expression.or;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigFunctionTest {
  private static final ConfigFunction.Signature<Contexts.Root, Integer> ROOT =
    signature(Contexts.Root.DESCRIPTION, Integer.class);
  private static final ConfigFunction.Signature<Contexts.ProcessFeature, Integer> FEATURE_SIGNATURE =
    signature(Contexts.ProcessFeature.DESCRIPTION, Integer.class);

  @Test
  void testConst() {
    assertEquals(1, constOf(1).apply(ScriptContext.empty()));
  }

  @Test
  void testVariable() {
    var feature = SimpleFeature.create(newPoint(0, 0), Map.of("a", "b", "c", 1), "source", "source_layer", 99);
    var context = new Contexts.ProcessFeature(feature, new TagValueProducer(Map.of()));
    // simple match
    assertEquals("source", variable(FEATURE_SIGNATURE.withOutput(String.class), "feature.source").apply(context));
    assertEquals("source_layer",
      variable(FEATURE_SIGNATURE.withOutput(String.class), "feature.source_layer").apply(context));
    assertEquals(99L, variable(FEATURE_SIGNATURE.withOutput(Long.class), "feature.id").apply(context));
    assertEquals(99, variable(FEATURE_SIGNATURE.withOutput(Integer.class), "feature.id").apply(context));
    assertEquals(99d, variable(FEATURE_SIGNATURE.withOutput(Double.class), "feature.id").apply(context));
    assertThrows(IllegalArgumentException.class, () -> variable(FEATURE_SIGNATURE, "missing"));
  }

  @Test
  void testCoalesce() {
    assertNull(coalesce(List.of()).apply(Contexts.root()));
    assertNull(coalesce(
      List.of(
        constOf(null)
      )).apply(Contexts.root()));
    assertEquals(2, coalesce(
      List.of(
        constOf(null),
        constOf(2)
      )).apply(Contexts.root()));
    assertEquals(1, coalesce(
      List.of(
        constOf(1),
        constOf(2)
      )).apply(Contexts.root()));
  }

  @Test
  void testDynamic() {
    assertEquals(1, expression(ROOT, "5 - 4").apply(Contexts.root()));
  }

  @Test
  void testMatch() {
    var feature = SimpleFeature.create(newPoint(0, 0), Map.of("a", "b", "c", 1));
    var context = new Contexts.ProcessFeature(feature, new TagValueProducer(Map.of()));
    // simple match
    assertEquals(2, match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1),
        DynamicBooleanExpression.dynamic("feature.tags.has('a', 'c')", FEATURE_SIGNATURE.in())),
      MultiExpression.entry(constOf(2),
        DynamicBooleanExpression.dynamic("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
    ))).apply(context));

    // dynamic fallback
    assertEquals(5, match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1),
        DynamicBooleanExpression.dynamic("feature.tags.has('a', 'c')", FEATURE_SIGNATURE.in())),
      MultiExpression.entry(constOf(2),
        DynamicBooleanExpression.dynamic("feature.tags.has('a', 'd')", FEATURE_SIGNATURE.in()))
    )), ConfigFunction.expression(FEATURE_SIGNATURE, "feature.tags.c + 4")).apply(context));

    // no fallback
    assertNull(match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1),
        DynamicBooleanExpression.dynamic("feature.tags.has('a', 'd')", FEATURE_SIGNATURE.in())),
      MultiExpression.entry(constOf(2),
        DynamicBooleanExpression.dynamic("feature.tags.has('a', 'e')", FEATURE_SIGNATURE.in()))
    ))).apply(context));

    // dynamic value
    assertEquals(2, match(
      FEATURE_SIGNATURE,
      MultiExpression.of(List.of(
        MultiExpression.entry(expression(FEATURE_SIGNATURE, "1 + size(feature.tags.a)"),
          Expression.matchAny("a", "b")),
        MultiExpression.entry(constOf(1), Expression.matchAny("a", "c"))
      ))
    ).apply(context));
  }

  @Test
  void testSimplifyCelFunction() {
    assertEquals(
      constOf(3),

      expression(FEATURE_SIGNATURE, "1+2").simplify()
    );
  }

  @Test
  void testSimplifyCelFunctionThatJustAccessesVar() {
    assertEquals(
      variable(FEATURE_SIGNATURE, "feature.id"),

      expression(FEATURE_SIGNATURE, "feature.id").simplify()
    );
    assertEquals(
      expression(FEATURE_SIGNATURE, "feature.tags.a"),

      expression(FEATURE_SIGNATURE, "feature.tags.a").simplify()
    );
  }

  @Test
  void testSimplifyCoalesce() {
    assertEquals(
      constOf(null),
      coalesce(List.of()).simplify()
    );
    assertEquals(
      constOf(null),
      coalesce(List.of(constOf(null))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1), constOf(2))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1), constOf(1))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1), constOf(2), constOf(1))).simplify()
    );
  }

  @Test
  void testSimplifyMatchAllFalse() {
    assertEquals(
      constOf(null),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          DynamicBooleanExpression.dynamic("1 > 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          DynamicBooleanExpression.dynamic("1 > 3", FEATURE_SIGNATURE.in()))
      ))).simplify()
    );
  }

  @Test
  void testSimplifyMatchAllFalseWithFallback() {
    assertEquals(
      constOf(3),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          DynamicBooleanExpression.dynamic("1 > 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          DynamicBooleanExpression.dynamic("1 > 3", FEATURE_SIGNATURE.in()))
      )), expression(FEATURE_SIGNATURE, "1+2")).simplify()
    );
  }

  @Test
  void testSimplifyRemoveCasesAfterTrueAndReplaceFallback() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(0),
          DynamicBooleanExpression.dynamic("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
      )), constOf(1)),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(0),
          DynamicBooleanExpression.dynamic("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(1),
          DynamicBooleanExpression.dynamic("1 < 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          DynamicBooleanExpression.dynamic("feature.tags.has('c', 'd')", FEATURE_SIGNATURE.in()))
      )), constOf(2)).simplify()
    );
  }

  @Test
  void testSimplifyRemoveFalseCases() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2),
          DynamicBooleanExpression.dynamic("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
      )), expression(FEATURE_SIGNATURE, "size(feature.tags.a)")),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          DynamicBooleanExpression.dynamic("1 > 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          DynamicBooleanExpression.dynamic("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
      )), expression(FEATURE_SIGNATURE, "size(feature.tags.a)")).simplify()
    );
  }

  @Test
  void testSimplifyMatchCondition() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b")))
      ), expression(FEATURE_SIGNATURE, "size(feature.tags.a)")),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), or(or(matchAny("a", "b"))))
      )), expression(FEATURE_SIGNATURE, "size(feature.tags.a)")).simplify()
    );
  }

  @Test
  void testSimplifyMatchResultFunction() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b")))
      ), expression(FEATURE_SIGNATURE, "size(feature.tags.a)")),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(coalesce(List.of(constOf(2))), matchAny("a", "b"))
      )), expression(FEATURE_SIGNATURE, "size(feature.tags.a)")).simplify()
    );
  }

  @Test
  void testSimplifyFallbackFunction() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b")))
      ), constOf(3)),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b"))
      )), expression(FEATURE_SIGNATURE, "1+2")).simplify()
    );
  }

  @Test
  void testSimplifyFirstTrue() {
    assertEquals(
      constOf(1),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          DynamicBooleanExpression.dynamic("1 < 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          DynamicBooleanExpression.dynamic("1 > 3", FEATURE_SIGNATURE.in()))
      ))).simplify()
    );
  }
}
